"""
20_precomputar_alpha1_contrafactual.py

Pre-computa o valor contrafactual de alpha1 (multa evitada) por municipio
costeiro. Para cada muni, "pergunta" a todos os autos de treino: "se voce
tivesse acontecido neste muni, quanto teria sido?" e tira a mediana das
previsoes (em log), voltando para R$ com exp.

Logica:
  1. Carrega X, y, feat_names via s17.carregar() (autos de oleo, 9966/00 +
     4136/2002, 2279 autos).
  2. Treina LightGBM com todos os anos <= latest (ou mantem 2025 no treino,
     dado que queremos o melhor contrafactual possivel para uso futuro).
  3. Para cada muni costeiro, copia X, sobrescreve apenas as colunas de
     caracteristicas DO MUNI (log_pop, log_pib_pc, infra, setor, cluster,
     ANP muni-level) e prediz. O restante (tipo_infracao, norma, SICAFI
     categoricals) vem da distribuicao real do treino — a mediana das
     predicoes equivale a marginalizar sobre tipos de auto.
  4. Grava em mb_alpha1_contrafactual_muni (code_muni, valor_rs, n_treino,
     ano_treino, calculado_em).

Saida:
  - tabela mb_alpha1_contrafactual_muni
  - log resumido por setor
"""
from __future__ import annotations

import importlib.util
import math
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import lightgbm as lgb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"


def p(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    spec = importlib.util.spec_from_file_location(
        "s17", ROOT / "scripts" / "17_treinar_modelos_alpha1.py"
    )
    s17 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(s17)
    X, y, feat_names, anos, _, _ = s17.carregar()
    anos_arr = np.array(anos, dtype=int)
    ano_max = int(anos_arr.max())
    p(f"dataset: {len(y)} autos, anos {int(anos_arr.min())}..{ano_max}")

    idx = {n: i for i, n in enumerate(feat_names)}
    col_log_pop = idx["log_pop"]
    col_log_pib = idx["log_pib_pc"]
    col_ref = idx["tem_refinaria"]
    col_term = idx["tem_terminal"]
    col_duto = idx["tem_duto"]
    col_eep = idx["tem_campo_eep"]
    col_anp_muni_n = idx["log_anp_muni_n"]
    col_anp_muni_vol = idx["log_anp_muni_vol"]
    col_anp_muni_max = idx["log_anp_muni_max"]
    col_setor = [idx[f"setor_{i}_{nm}"] for i, nm in
                 enumerate(["norte", "nordeste", "leste", "sudeste", "sul"], start=1)]
    col_cluster = [idx["cluster_0_infra"], idx["cluster_1"],
                   idx["cluster_2"], idx["cluster_3"]]

    params = {
        "objective": "regression", "metric": "mae",
        "learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 30,
        "feature_fraction": 0.7, "bagging_fraction": 0.8, "bagging_freq": 5,
        "lambda_l1": 0.5, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
    }
    w = np.ones(len(y))
    lgb_tr = lgb.Dataset(X, label=y, weight=w, feature_name=feat_names)
    booster = lgb.train(params, lgb_tr, num_boost_round=300)
    p(f"booster treinado em {len(y)} autos (ate {ano_max})")

    con = sqlite3.connect(DB)

    socio_all = con.execute(
        "SELECT code_muni, ano, pop, pib_rs FROM mb_muni_socio_anual"
    ).fetchall()
    socio: dict[str, tuple[int, float, float]] = {}
    for code, ano, pop, pib in socio_all:
        if pop is None or pib is None or pop <= 0:
            continue
        cur = socio.get(code)
        if cur is None or int(ano) > cur[0]:
            socio[code] = (int(ano), float(pop), float(pib))

    infra = {r[0]: tuple(r[1:]) for r in con.execute(
        "SELECT code_muni, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep "
        "FROM mb_infra_oleo"
    ).fetchall()}

    setores_muni = {r[0]: r[1] for r in con.execute(
        "SELECT code_muni, setor_pngc FROM municipios_brasil WHERE setor_pngc IS NOT NULL"
    ).fetchall()}
    clusters_muni = {r[0]: r[1] for r in con.execute(
        "SELECT code_muni, cluster_id FROM mb_estratos_alpha1"
    ).fetchall()}

    anp_muni: dict[str, tuple[int, float, float]] = {}
    try:
        for r in con.execute(
            "SELECT code_muni, SUM(n_incidentes), SUM(vol_oleo_m3), MAX(vol_max_m3) "
            "FROM mb_anp_incidentes GROUP BY code_muni"
        ).fetchall():
            anp_muni[r[0]] = (int(r[1] or 0), float(r[2] or 0), float(r[3] or 0))
    except sqlite3.OperationalError:
        pass

    costeiros = con.execute(
        "SELECT code_muni, nome, uf FROM municipios_costeiros"
    ).fetchall()
    p(f"municipios costeiros: {len(costeiros)}")

    con.execute("DROP TABLE IF EXISTS mb_alpha1_contrafactual_muni")
    con.execute("""
        CREATE TABLE mb_alpha1_contrafactual_muni (
            code_muni     TEXT PRIMARY KEY,
            nome          TEXT,
            uf            TEXT,
            valor_rs      REAL NOT NULL,
            n_treino      INTEGER NOT NULL,
            ano_treino    INTEGER NOT NULL,
            calculado_em  TEXT NOT NULL,
            editado_manual INTEGER DEFAULT 0
        )
    """)

    inserts = []
    pulados = 0
    agora = datetime.utcnow().isoformat(timespec="seconds")
    for code, nome, uf in costeiros:
        s = socio.get(code)
        if s is None:
            pulados += 1
            continue
        _, pop, pib = s
        pib_pc = pib / pop if pop > 0 else None
        if pib_pc is None or pib_pc <= 0:
            pulados += 1
            continue
        log_pop = math.log(pop)
        log_pib_pc = math.log(pib_pc)
        setor = setores_muni.get(code)
        cluster = clusters_muni.get(code)
        if setor is None or cluster is None:
            pulados += 1
            continue
        inf = infra.get(code, (0, 0, 0, 0))
        mn, mv, mx = anp_muni.get(code, (0, 0.0, 0.0))

        Xs = X.copy()
        Xs[:, col_log_pop] = log_pop
        Xs[:, col_log_pib] = log_pib_pc
        Xs[:, col_ref] = float(inf[0])
        Xs[:, col_term] = float(inf[1])
        Xs[:, col_duto] = float(inf[2])
        Xs[:, col_eep] = float(inf[3])
        Xs[:, col_anp_muni_n] = math.log1p(mn)
        Xs[:, col_anp_muni_vol] = math.log1p(mv)
        Xs[:, col_anp_muni_max] = math.log1p(mx)
        for i, ci in enumerate(col_setor, start=1):
            Xs[:, ci] = 1.0 if setor == i else 0.0
        for i, ci in enumerate(col_cluster):
            Xs[:, ci] = 1.0 if cluster == i else 0.0

        preds_log = booster.predict(Xs)
        valor = float(math.exp(float(np.median(preds_log))))
        inserts.append((code, nome, uf, round(valor, 2),
                        len(y), ano_max, agora, 0))

    con.executemany(
        "INSERT INTO mb_alpha1_contrafactual_muni VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        inserts,
    )

    con.execute(
        "INSERT INTO metadados_atualizacao "
        "(fonte, nome_humano, orgao, ultima_safra, atualizado_em, url, url_portal, "
        "descricao_uso, script, observacoes_metodologicas) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(fonte) DO UPDATE SET "
        "nome_humano=excluded.nome_humano, orgao=excluded.orgao, "
        "ultima_safra=excluded.ultima_safra, atualizado_em=excluded.atualizado_em, "
        "descricao_uso=excluded.descricao_uso, script=excluded.script, "
        "observacoes_metodologicas=excluded.observacoes_metodologicas",
        (
            "icseiom_alpha1_contrafactual",
            "Alpha1 contrafactual por muni",
            "ICSEIOM",
            str(ano_max),
            agora,
            "",
            "",
            "Valor previsto de multa de oleo (Lei 9966/00 + 4136/2002) para "
            "um auto tipico em cada muni costeiro. Usado como α₁ no Ramo B "
            "(evento nao-poluidor) do calculo ICSEIOM.",
            "scripts/20_precomputar_alpha1_contrafactual.py",
            f"Modelo LightGBM treinado em {len(y)} autos. Mediana sobre "
            "marginalizacao de tipo_infracao/norma/SICAFI. Editavel pelo "
            "operador via /admin/fontes.",
        ),
    )
    con.commit()

    por_setor = con.execute("""
        SELECT mb.setor_pngc, COUNT(*), ROUND(AVG(cf.valor_rs), 2)
        FROM mb_alpha1_contrafactual_muni cf
        JOIN municipios_brasil mb ON mb.code_muni = cf.code_muni
        GROUP BY mb.setor_pngc
        ORDER BY mb.setor_pngc
    """).fetchall()
    con.close()

    p(f"[OK] mb_alpha1_contrafactual_muni: {len(inserts)} munis "
      f"({pulados} pulados por falta de features)")
    for s, n, med in por_setor:
        p(f"     setor {s}: {n} munis, media R$ {med:,.0f}")


if __name__ == "__main__":
    main()
