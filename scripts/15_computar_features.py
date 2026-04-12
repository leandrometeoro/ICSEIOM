"""
15_computar_features.py

Constroi as tabelas de features usadas pelos modelos de alpha1:

  mb_auto_features
    Unidade de observacao = (code_muni, ano). Uma linha por celula (municipio,
    ano) com autos de infracao registrados na mb_alpha1_multa. Usada como
    observacao na regressao do valor_medio_real.

  mb_features_muni
    Unidade = municipio. Uma linha por municipio costeiro. Usada no clustering
    PCA + GMM por setor PNGC.

Correcao IPCA: todo valor monetario historico (multa, PIB) e convertido para
reais do mes corrente via fator acumulado calculado a partir da serie ipca_mensal
(BCB SGS 433).

Fontes usadas (ja carregadas por scripts anteriores):
  - mb_alpha1_multa           (script 10, IBAMA SIFISC filtro estrito)
  - mb_muni_socio_anual       (script 13, pop/PIB SIDRA)
  - mb_infra_oleo             (script 14, curadoria ANP/ANTAQ)
  - ipca_mensal               (script 11, BCB SGS 433)
  - municipios_brasil         (script 08/09/12, malha + costeiro + setor_pngc)

Observacao metodologica: trabalhamos no nivel agregado (code_muni, ano) porque
a tabela mb_alpha1_multa ja vem agregada do script 10. Alternativa superior
seria reprocessar os CSVs do IBAMA preservando cada auto individualmente; fica
para iteracao futura se o sinal por celula for insuficiente.
"""
import sqlite3
import statistics
import sys
from datetime import datetime, date
from math import log
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

# Mes-alvo para trazer todos os valores historicos ao "hoje".
# Usamos o ultimo mes disponivel na serie ipca_mensal dinamicamente.

def p(msg: str) -> None:
    print(msg, flush=True)


def build_ipca_index(con: sqlite3.Connection) -> tuple[dict[tuple[int,int], float], tuple[int,int]]:
    rows = con.execute(
        "SELECT ano, mes, valor_pct FROM ipca_mensal ORDER BY ano, mes"
    ).fetchall()
    if not rows:
        raise RuntimeError("ipca_mensal vazia - rode scripts/11_baixar_ipca.py")
    idx: dict[tuple[int,int], float] = {}
    acc = 1.0
    for ano, mes, pct in rows:
        acc *= (1.0 + float(pct) / 100.0)
        idx[(ano, mes)] = acc
    ultimo = (rows[-1][0], rows[-1][1])
    return idx, ultimo


def fator_ipca(idx: dict[tuple[int,int], float], ano_origem: int, mes_origem: int,
               ano_alvo: int, mes_alvo: int) -> float:
    """Fator multiplicativo para converter valor de (ano_origem, mes_origem)
    para (ano_alvo, mes_alvo). Assume que o valor esta referenciado ao fim do
    mes de origem. Se origem posterior ao alvo, retorna 1."""
    origem = (ano_origem, mes_origem)
    alvo = (ano_alvo, mes_alvo)
    if origem >= alvo:
        return 1.0
    a = idx.get(origem)
    b = idx.get(alvo)
    if a is None:
        # anterior ao inicio da serie: usa o primeiro ponto disponivel
        primeiro = min(idx.keys())
        a = idx[primeiro]
    if b is None:
        b = idx[max(idx.keys())]
    return b / a


def load_socio(con: sqlite3.Connection) -> dict[tuple[str,int], tuple[int|None, float|None]]:
    rows = con.execute(
        "SELECT code_muni, ano, pop, pib_rs FROM mb_muni_socio_anual"
    ).fetchall()
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


def nearest_year_lookup(d: dict[tuple[str,int], tuple], code: str, ano: int,
                        campo_idx: int, anos_disponiveis: dict[str, list[int]]) -> float | None:
    """Lookup com fallback pelo ano mais proximo disponivel para aquele muni."""
    v = d.get((code, ano))
    if v is not None and v[campo_idx] is not None:
        return v[campo_idx]
    anos = anos_disponiveis.get(code, [])
    if not anos:
        return None
    nearest = min(anos, key=lambda a: abs(a - ano))
    alt = d.get((code, nearest))
    if alt is None:
        return None
    return alt[campo_idx]


def build_anos_disponiveis(socio: dict) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    pop_anos: dict[str, list[int]] = {}
    pib_anos: dict[str, list[int]] = {}
    for (code, ano), (pop, pib) in socio.items():
        if pop is not None:
            pop_anos.setdefault(code, []).append(ano)
        if pib is not None:
            pib_anos.setdefault(code, []).append(ano)
    return pop_anos, pib_anos


def ensure_tables(con: sqlite3.Connection) -> None:
    con.execute("DROP TABLE IF EXISTS mb_auto_features")
    con.execute("""
        CREATE TABLE mb_auto_features (
            code_muni            TEXT NOT NULL,
            ano                  INTEGER NOT NULL,
            n_autos              INTEGER NOT NULL,
            valor_nominal_rs     REAL NOT NULL,
            valor_real_rs        REAL NOT NULL,
            valor_medio_real     REAL NOT NULL,
            log_valor_medio_real REAL NOT NULL,
            pop                  INTEGER,
            pib_nominal_rs       REAL,
            pib_real_rs          REAL,
            pib_per_capita_real  REAL,
            multa_frac_pib       REAL,
            multa_per_capita_real REAL,
            setor_pngc           INTEGER,
            tem_infra            INTEGER DEFAULT 0,
            tem_refinaria        INTEGER DEFAULT 0,
            tem_terminal         INTEGER DEFAULT 0,
            tem_duto             INTEGER DEFAULT 0,
            tem_campo_eep        INTEGER DEFAULT 0,
            PRIMARY KEY (code_muni, ano)
        )
    """)
    con.execute("CREATE INDEX idx_mb_af_setor ON mb_auto_features(setor_pngc)")
    con.execute("CREATE INDEX idx_mb_af_ano ON mb_auto_features(ano)")

    con.execute("DROP TABLE IF EXISTS mb_features_muni")
    con.execute("""
        CREATE TABLE mb_features_muni (
            code_muni              TEXT PRIMARY KEY,
            setor_pngc             INTEGER,
            tem_infra              INTEGER DEFAULT 0,
            n_autos_total          INTEGER NOT NULL,
            mediana_multa_real     REAL,
            media_multa_real       REAL,
            max_multa_real         REAL,
            pop_atual              INTEGER,
            pib_per_capita_real    REAL,
            log_pop                REAL,
            log_pib_pc             REAL,
            log_n_autos            REAL,
            log_mediana_multa      REAL
        )
    """)
    con.execute("CREATE INDEX idx_mb_fm_setor ON mb_features_muni(setor_pngc)")


def main():
    con = sqlite3.connect(DB)

    p("construindo indice IPCA...")
    ipca_idx, ultimo_ipca = build_ipca_index(con)
    ano_alvo, mes_alvo = ultimo_ipca
    p(f"  mes-alvo IPCA: {ano_alvo}-{mes_alvo:02d}")

    p("carregando tabelas auxiliares...")
    socio = load_socio(con)
    pop_anos, pib_anos = build_anos_disponiveis(socio)

    infra = {r[0]: tuple(r[1:]) for r in con.execute(
        "SELECT code_muni, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep "
        "FROM mb_infra_oleo"
    ).fetchall()}

    setores = {r[0]: r[1] for r in con.execute(
        "SELECT code_muni, setor_pngc FROM municipios_brasil WHERE setor_pngc IS NOT NULL"
    ).fetchall()}

    # populacao atual (mais recente disponivel) e PIB per capita atual corrigido
    # para todos os munis costeiros — usado no mb_features_muni como snapshot.
    max_pop_por_muni: dict[str, tuple[int, int]] = {}
    max_pib_por_muni: dict[str, tuple[int, float]] = {}
    for (code, ano), (pop, pib) in socio.items():
        if pop is not None:
            cur = max_pop_por_muni.get(code)
            if cur is None or ano > cur[0]:
                max_pop_por_muni[code] = (ano, pop)
        if pib is not None:
            cur = max_pib_por_muni.get(code)
            if cur is None or ano > cur[0]:
                max_pib_por_muni[code] = (ano, pib)

    ensure_tables(con)

    p("processando mb_alpha1_multa...")
    rows = con.execute(
        "SELECT a.code_muni, a.ano, a.valor_rs, a.n_autos "
        "FROM mb_alpha1_multa a "
        "JOIN municipios_brasil m ON m.code_muni = a.code_muni "
        "WHERE m.is_costeiro = 1 AND a.n_autos > 0 "
        "ORDER BY a.code_muni, a.ano"
    ).fetchall()
    p(f"  {len(rows)} celulas (code_muni, ano)")

    inserts_auto = []
    for code, ano, valor_nom, n_autos in rows:
        # IPCA: assume mid-year (julho) como mes de referencia
        fator = fator_ipca(ipca_idx, ano, 7, ano_alvo, mes_alvo)
        valor_real = valor_nom * fator
        valor_medio = valor_real / n_autos

        pop = nearest_year_lookup(socio, code, ano, 0, pop_anos)
        pib_nom = nearest_year_lookup(socio, code, ano, 1, pib_anos)
        pib_real = None
        pib_pc_real = None
        multa_frac_pib = None
        multa_percap = None
        if pib_nom is not None:
            pib_real = pib_nom * fator
            if pop:
                pib_pc_real = pib_real / pop
            if pib_nom > 0:
                multa_frac_pib = valor_nom / pib_nom
        if pop and pop > 0:
            multa_percap = valor_real / pop

        setor = setores.get(code)
        inf = infra.get(code, (0, 0, 0, 0))
        tem_infra = 1 if any(inf) else 0

        inserts_auto.append((
            code, ano, int(n_autos), float(valor_nom), float(valor_real),
            float(valor_medio), log(max(valor_medio, 1.0)),
            int(pop) if pop else None,
            float(pib_nom) if pib_nom is not None else None,
            float(pib_real) if pib_real is not None else None,
            float(pib_pc_real) if pib_pc_real is not None else None,
            float(multa_frac_pib) if multa_frac_pib is not None else None,
            float(multa_percap) if multa_percap is not None else None,
            setor,
            tem_infra,
            int(inf[0]), int(inf[1]), int(inf[2]), int(inf[3]),
        ))

    con.executemany(
        "INSERT INTO mb_auto_features VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        inserts_auto,
    )
    p(f"  mb_auto_features: {len(inserts_auto)} linhas inseridas")

    p("agregando mb_features_muni...")
    # Agrega por municipio. Para cada muni com pelo menos 1 celula,
    # calcula mediana/media/max do valor_medio_real e count de autos.
    por_muni: dict[str, list] = {}
    for row in inserts_auto:
        code = row[0]
        valor_medio_real = row[5]
        n_autos = row[2]
        por_muni.setdefault(code, []).append((valor_medio_real, n_autos))

    # tambem inserimos municipios costeiros SEM autos, com features soh de pop/pib/infra
    costeiros = [r[0] for r in con.execute(
        "SELECT code_muni FROM municipios_brasil WHERE is_costeiro = 1"
    ).fetchall()]

    inserts_muni = []
    for code in costeiros:
        setor = setores.get(code)
        inf = infra.get(code, (0, 0, 0, 0))
        tem_infra = 1 if any(inf) else 0

        autos = por_muni.get(code, [])
        if autos:
            valores = [v for v, _ in autos]
            n_total = sum(n for _, n in autos)
            med = statistics.median(valores)
            mean = statistics.mean(valores)
            mx = max(valores)
        else:
            n_total = 0
            med = mean = mx = None

        pop_atual_pair = max_pop_por_muni.get(code)
        pib_atual_pair = max_pib_por_muni.get(code)
        pop_atual = pop_atual_pair[1] if pop_atual_pair else None
        pib_pc_atual = None
        if pib_atual_pair and pop_atual:
            ano_pib = pib_atual_pair[0]
            fator_atual = fator_ipca(ipca_idx, ano_pib, 7, ano_alvo, mes_alvo)
            pib_pc_atual = (pib_atual_pair[1] * fator_atual) / pop_atual

        log_pop = log(pop_atual) if pop_atual and pop_atual > 0 else None
        log_pib_pc = log(pib_pc_atual) if pib_pc_atual and pib_pc_atual > 0 else None
        log_n_autos = log(1 + n_total)
        log_med = log(med) if med and med > 0 else None

        inserts_muni.append((
            code, setor, tem_infra, n_total,
            med, mean, mx,
            pop_atual, pib_pc_atual,
            log_pop, log_pib_pc, log_n_autos, log_med,
        ))

    con.executemany(
        "INSERT INTO mb_features_muni VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        inserts_muni,
    )
    p(f"  mb_features_muni: {len(inserts_muni)} linhas inseridas")

    # Diagnostico
    stats = con.execute("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN mediana_multa_real IS NOT NULL THEN 1 ELSE 0 END) com_multa,
            SUM(CASE WHEN tem_infra = 1 THEN 1 ELSE 0 END) com_infra,
            SUM(CASE WHEN setor_pngc IS NOT NULL THEN 1 ELSE 0 END) com_setor
        FROM mb_features_muni
    """).fetchone()
    p("")
    p("[OK] features prontas")
    p(f"  costeiros totais : {stats[0]}")
    p(f"  com multa hist.  : {stats[1]}")
    p(f"  com infra oleo   : {stats[2]}")
    p(f"  com setor PNGC   : {stats[3]}")

    # registra fonte metodologica
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
            "icseiom_mb_features",
            "Features ICSEIOM alpha1 (auto + municipio)",
            "ICSEIOM",
            f"{min(r[1] for r in rows)}-{max(r[1] for r in rows)}" if rows else "—",
            datetime.utcnow().isoformat(timespec="seconds"),
            "",
            "",
            "Tabelas derivadas mb_auto_features e mb_features_muni, join de IBAMA, "
            "SIDRA pop/PIB, curadoria infra e setorizacao PNGC, com correcao IPCA.",
            "scripts/15_computar_features.py",
            f"Unidade de observacao do regressor = (code_muni, ano). Multa individual "
            f"estimada pela media sum/n_autos. Pop/PIB com fallback para ano mais "
            f"proximo disponivel quando lacuna (censos). IPCA alvo: {ano_alvo}-{mes_alvo:02d}.",
        ),
    )
    con.commit()
    con.close()


if __name__ == "__main__":
    main()
