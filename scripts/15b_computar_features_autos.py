"""
15b_computar_features_autos.py

Constroi a tabela de features per-auto para o modelo alpha1 v2.

Entrada:
  mb_alpha1_autos     (uma linha por auto de infracao individual)
  mb_muni_socio_anual (pop, pib por (muni, ano))
  mb_infra_oleo       (flags tem_refinaria / terminal / duto / campo_eep)
  municipios_brasil   (setor_pngc)
  mb_estratos_alpha1  (setor_pngc + cluster_id data-driven)
  ipca_mensal         (correcao IPCA)

Saida:
  mb_alpha1_auto_feat — uma linha por auto, com:
    seq_auto, code_muni, ano, mes
    valor_nominal_rs, valor_real_rs, log_valor_real
    tipo_infracao, tp_norma, nu_norma, artigo, gravidade, match_via
    log_pop, log_pib_pc, setor_pngc, cluster_id
    tem_infra, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep

Correcao IPCA usa o mes do fato (quando disponivel), nao mid-year.
Pop/PIB usam nearest-year lookup (mesma logica do script 15).
"""
import sqlite3
from datetime import datetime
from math import log
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"


def p(msg: str) -> None:
    print(msg, flush=True)


def build_ipca_index(con):
    rows = con.execute(
        "SELECT ano, mes, valor_pct FROM ipca_mensal ORDER BY ano, mes"
    ).fetchall()
    idx = {}
    acc = 1.0
    for ano, mes, pct in rows:
        acc *= (1.0 + float(pct) / 100.0)
        idx[(ano, mes)] = acc
    return idx, (rows[-1][0], rows[-1][1])


def fator_ipca(idx, ano_o, mes_o, ano_a, mes_a):
    origem = (ano_o, mes_o)
    alvo = (ano_a, mes_a)
    if origem >= alvo:
        return 1.0
    a = idx.get(origem)
    b = idx.get(alvo)
    if a is None:
        a = idx[min(idx.keys())]
    if b is None:
        b = idx[max(idx.keys())]
    return b / a


def nearest_year(d, code, ano, campo_idx, anos_disp):
    v = d.get((code, ano))
    if v is not None and v[campo_idx] is not None:
        return v[campo_idx]
    anos = anos_disp.get(code, [])
    if not anos:
        return None
    nearest = min(anos, key=lambda a: abs(a - ano))
    alt = d.get((code, nearest))
    if alt is None:
        return None
    return alt[campo_idx]


def ensure_table(con):
    con.execute("DROP TABLE IF EXISTS mb_alpha1_auto_feat")
    con.execute("""
        CREATE TABLE mb_alpha1_auto_feat (
            seq_auto       TEXT PRIMARY KEY,
            code_muni      TEXT NOT NULL,
            ano            INTEGER NOT NULL,
            mes            INTEGER NOT NULL,
            valor_nominal_rs REAL NOT NULL,
            valor_real_rs  REAL NOT NULL,
            log_valor_real REAL NOT NULL,
            tipo_infracao  TEXT,
            tp_norma       TEXT,
            nu_norma       TEXT,
            artigo         TEXT,
            gravidade      TEXT,
            match_via      TEXT,
            log_pop        REAL,
            log_pib_pc     REAL,
            setor_pngc     INTEGER,
            cluster_id     INTEGER,
            tem_infra      INTEGER DEFAULT 0,
            tem_refinaria  INTEGER DEFAULT 0,
            tem_terminal   INTEGER DEFAULT 0,
            tem_duto       INTEGER DEFAULT 0,
            tem_campo_eep  INTEGER DEFAULT 0,
            anp_n_incidentes INTEGER DEFAULT 0,
            anp_vol_oleo_m3  REAL DEFAULT 0,
            anp_vol_max_m3   REAL DEFAULT 0,
            anp_muni_n_total INTEGER DEFAULT 0,
            anp_muni_vol_total REAL DEFAULT 0,
            anp_muni_vol_max REAL DEFAULT 0,
            tipo_multa          TEXT,
            motivacao_conduta   TEXT,
            efeito_meio_amb     TEXT,
            efeito_saude        TEXT,
            passivel_recup      TEXT,
            qt_area             REAL,
            classificacao_area  TEXT,
            tp_pessoa_infrator  TEXT,
            tipo_acao           TEXT,
            unid_arrecadacao    TEXT,
            ds_biomas           TEXT
        )
    """)
    con.execute("CREATE INDEX idx_mb_a1af_ano ON mb_alpha1_auto_feat(ano)")
    con.execute("CREATE INDEX idx_mb_a1af_setor ON mb_alpha1_auto_feat(setor_pngc)")
    con.execute("CREATE INDEX idx_mb_a1af_tipo ON mb_alpha1_auto_feat(tipo_infracao)")


def main():
    con = sqlite3.connect(DB)
    ipca, (ano_alvo, mes_alvo) = build_ipca_index(con)
    p(f"IPCA alvo: {ano_alvo}-{mes_alvo:02d}")

    socio = {(r[0], r[1]): (r[2], r[3]) for r in con.execute(
        "SELECT code_muni, ano, pop, pib_rs FROM mb_muni_socio_anual"
    ).fetchall()}
    pop_anos, pib_anos = {}, {}
    for (code, ano), (pop, pib) in socio.items():
        if pop is not None: pop_anos.setdefault(code, []).append(ano)
        if pib is not None: pib_anos.setdefault(code, []).append(ano)

    infra = {r[0]: tuple(r[1:]) for r in con.execute(
        "SELECT code_muni, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep "
        "FROM mb_infra_oleo"
    ).fetchall()}

    setores = {r[0]: r[1] for r in con.execute(
        "SELECT code_muni, setor_pngc FROM municipios_brasil WHERE setor_pngc IS NOT NULL"
    ).fetchall()}

    clusters = {r[0]: r[1] for r in con.execute(
        "SELECT code_muni, cluster_id FROM mb_estratos_alpha1"
    ).fetchall()}

    # ANP incidentes por (muni, ano) — tenta tabela opcional
    anp: dict[tuple[str, int], tuple[int, float, float]] = {}
    anp_muni: dict[str, tuple[int, float, float]] = {}
    try:
        for r in con.execute(
            "SELECT code_muni, ano, n_incidentes, vol_oleo_m3, vol_max_m3 "
            "FROM mb_anp_incidentes"
        ).fetchall():
            anp[(r[0], int(r[1]))] = (int(r[2]), float(r[3]), float(r[4]))
        p(f"ANP incidentes: {len(anp)} celulas carregadas")
        for r in con.execute(
            "SELECT code_muni, SUM(n_incidentes), SUM(vol_oleo_m3), MAX(vol_max_m3) "
            "FROM mb_anp_incidentes GROUP BY code_muni"
        ).fetchall():
            anp_muni[r[0]] = (int(r[1] or 0), float(r[2] or 0), float(r[3] or 0))
        p(f"ANP muni-level: {len(anp_muni)} munis")
    except sqlite3.OperationalError:
        p("[aviso] mb_anp_incidentes nao existe ainda — features ANP ficam em 0")

    autos = con.execute("""
        SELECT seq_auto, code_muni, ano, mes, valor_rs,
               tipo_infracao, tp_norma, nu_norma, artigo, gravidade, match_via,
               tipo_multa, motivacao_conduta, efeito_meio_amb, efeito_saude,
               passivel_recup, qt_area, classificacao_area,
               tp_pessoa_infrator, tipo_acao, unid_arrecadacao, ds_biomas
        FROM mb_alpha1_autos
    """).fetchall()
    p(f"autos a processar: {len(autos)}")

    ensure_table(con)

    inserts = []
    for r in autos:
        (seq, code, ano, mes, valor_nom, tipo, tp, nu, art, grav, via,
         tipo_multa, motivacao, efeito_meio, efeito_saude, passivel,
         qt_area, class_area, tp_pessoa, tipo_acao, unid_arr, biomas) = r

        fator = fator_ipca(ipca, int(ano), int(mes), ano_alvo, mes_alvo)
        valor_real = float(valor_nom) * fator
        if valor_real <= 0:
            continue

        pop = nearest_year(socio, code, int(ano), 0, pop_anos)
        pib_nom = nearest_year(socio, code, int(ano), 1, pib_anos)
        pib_pc_real = None
        if pib_nom is not None and pop and pop > 0:
            pib_pc_real = (pib_nom * fator) / pop

        log_pop = log(pop) if pop and pop > 0 else None
        log_pib_pc = log(pib_pc_real) if pib_pc_real and pib_pc_real > 0 else None

        inf = infra.get(code, (0, 0, 0, 0))
        tem_infra = 1 if any(inf) else 0

        anp_n, anp_vol, anp_max = anp.get((code, int(ano)), (0, 0.0, 0.0))
        anp_mn, anp_mv, anp_mx = anp_muni.get(code, (0, 0.0, 0.0))

        inserts.append((
            seq, code, int(ano), int(mes),
            float(valor_nom), float(valor_real), float(log(max(valor_real, 1.0))),
            tipo, tp, nu, art, grav, via,
            log_pop, log_pib_pc,
            setores.get(code), clusters.get(code),
            tem_infra, int(inf[0]), int(inf[1]), int(inf[2]), int(inf[3]),
            int(anp_n), float(anp_vol), float(anp_max),
            int(anp_mn), float(anp_mv), float(anp_mx),
            tipo_multa, motivacao, efeito_meio, efeito_saude, passivel,
            qt_area, class_area, tp_pessoa, tipo_acao, unid_arr, biomas,
        ))

    con.executemany(
        "INSERT INTO mb_alpha1_auto_feat VALUES (" + ",".join(["?"] * 39) + ")",
        inserts,
    )
    con.commit()

    n = con.execute("SELECT COUNT(*) FROM mb_alpha1_auto_feat").fetchone()[0]
    nn = con.execute("SELECT COUNT(DISTINCT tipo_infracao) FROM mb_alpha1_auto_feat").fetchone()[0]
    ng = con.execute("SELECT COUNT(DISTINCT nu_norma) FROM mb_alpha1_auto_feat WHERE nu_norma <> ''").fetchone()[0]

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
            "icseiom_mb_alpha1_auto_feat",
            "Features per-auto alpha1",
            "ICSEIOM",
            "—",
            datetime.utcnow().isoformat(timespec="seconds"),
            "",
            "",
            "Features per-auto usadas no modelo de alpha1 v2. Target = "
            "log(valor_real_rs). Feature-chave = tipo_infracao / norma / artigo.",
            "scripts/15b_computar_features_autos.py",
            f"{n} autos com features, {nn} tipos de infracao distintos, "
            f"{ng} normas distintas. Correcao IPCA pelo mes do fato.",
        ),
    )
    con.commit()
    con.close()
    p(f"[OK] mb_alpha1_auto_feat: {n} autos")
    p(f"     tipos distintos: {nn}")
    p(f"     normas distintas: {ng}")


if __name__ == "__main__":
    main()
