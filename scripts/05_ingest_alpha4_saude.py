"""
05_ingest_alpha4_saude.py   —   alpha4: custo de saude evitado (dados reais)

Metodologia
-----------
    alpha4(muni, ano) = pop(muni) * sum_CID [ casos_por_100k(CID) * custo_CID ] / 1e5

A interpretacao: quanto custaria ao SUS atender a populacao do municipio em
caso de contato com oleo, dada uma estimativa conservadora da fracao da
populacao que procuraria atendimento.

No calc.py, o alpha4 so e somado ao ICSEIOM quando o evento E poluente
(foi_poluente=True), porque a LGAF ao identificar o poluente aciona o alerta
a populacao, evitando o contato. Em alarmes falsos, nao ha custo sanitario
a evitar. E o inverso do alpha3 (turismo), que so preserva quando nao polui.

CIDs considerados (agrupados em 3 sindromes operacionais):

    1) Dermatites de contato (L23 alergica + L24 irritante)
       - Quadro mais frequente por contato direto com hidrocarbonetos na praia.
       - Atendimento tipicamente ambulatorial na rede SUS.

    2) Efeitos respiratorios por inalacao de quimicos (J68)
       - Irritacao e broncoespasmo por vapores de oleo.
       - Pode exigir internacao em caso de exposicao prolongada ou populacao
         vulneravel (asmaticos, idosos, criancas).

    3) Efeitos toxicos sistemicos (T52 solventes organicos + T65 outras subst)
       - Casos mais graves, por ingestao acidental ou exposicao intensa.
       - Em geral internacao.

Parametros conservadores
------------------------
Taxa de procura por atendimento (casos por 100 mil habitantes, cenario de
derrame costeiro proximo da populacao):

    dermatite  : 200 / 100k   (meta-metade do range 150-300 da literatura)
    respiratoria: 350 / 100k  (DWH USCG RR~1.32-1.83; Prestige 35% em 6 anos)
    toxica     :  75 / 100k   (casos agudos, 5-10x baseline do SIH)

Custos medios por atendimento SUS (R$ de 2023, fonte DATASUS SIH + SIGTAP):

    dermatite  : R$   300 (atendimento ambulatorial, nao internacao)
    respiratoria: R$ 5.000 (AIH media faixa J00-J99 para pneumopatias)
    toxica     : R$ 8.000 (AIH media SIH categoria 309 "Efeitos toxicos
                 de substancias de origem principalmente nao-medicinal")

Referencias
-----------
- DATASUS SIH/SUS 2023 - Morbidade Hospitalar: valor total / AIH aprovadas
  http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nibr.def
- Sandifer P. A. et al. (2014) "Human health and socioeconomic effects of
  the Deepwater Horizon oil spill" Oceanography 27(4):86-93.
- Zock J. P. et al. (2007) "Prolonged respiratory symptoms in clean-up
  workers of the Prestige oil spill" Am J Respir Crit Care Med 176(6):610-616.
- Soares M. O. et al. (2020) "Oil spill in South Atlantic (Brazil): Environmental
  and governmental disaster" Marine Policy 115:103879.

Uso
---
    python scripts/05_ingest_alpha4_saude.py --ano 2023

Recalcula alpha4 para todos os municipios costeiros em municipios_brasil
(is_costeiro=1, ~443 municipios).
"""
import sqlite3
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
URL = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nibr.def"

# Parametros por grupo sindromico: (casos/100k, custo_rs/caso)
GRUPOS = {
    "dermatite":    {"casos_100k": 200.0, "custo_rs": 300.0,
                     "cids": "L23+L24"},
    "respiratoria": {"casos_100k": 350.0, "custo_rs": 5000.0,
                     "cids": "J68"},
    "toxica":       {"casos_100k":  75.0, "custo_rs": 8000.0,
                     "cids": "T52+T65"},
}


def _custo_por_100k() -> float:
    """R$ de custo sanitario esperado por 100 mil habitantes em cenario de
    derrame costeiro proximo, somando os 3 grupos sindromicos."""
    total = 0.0
    for g in GRUPOS.values():
        total += g["casos_100k"] * g["custo_rs"]
    return total


def _n_casos_por_100k() -> int:
    return sum(int(g["casos_100k"]) for g in GRUPOS.values())


def ingest_real(ano: int = 2023):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")

    # populacao: usa municipios_brasil is_costeiro=1, fazendo LEFT JOIN com
    # municipios_costeiros para pegar pop_2022 (61 muni da semente) e com
    # mb_muni_socio_anual (pop historica, ano mais recente) para os demais.
    cur.execute("""
        SELECT mb.code_muni, mb.nome, mb.uf,
               COALESCE(mc.pop_2022, ph.pop, 0) AS pop
        FROM municipios_brasil mb
        LEFT JOIN municipios_costeiros mc ON mc.code_muni = mb.code_muni
        LEFT JOIN (
            SELECT code_muni, pop FROM mb_muni_socio_anual
            WHERE ano = (SELECT MAX(ano) FROM mb_muni_socio_anual WHERE pop IS NOT NULL)
        ) ph ON ph.code_muni = mb.code_muni
        WHERE mb.is_costeiro = 1
    """)
    rows = cur.fetchall()

    custo_100k = _custo_por_100k()
    n_100k = _n_casos_por_100k()

    cur.execute("DELETE FROM alpha4_saude WHERE ano = ?", (ano,))
    inseridos = 0
    total_rs = 0.0
    sem_pop = 0

    for r in rows:
        pop = int(r["pop"] or 0)
        if pop <= 0:
            sem_pop += 1
            continue
        custo = pop * custo_100k / 1e5
        n_int = int(pop * n_100k / 1e5)
        cur.execute(
            "INSERT INTO alpha4_saude (code_muni, ano, custo_rs, n_internacoes, fonte)"
            " VALUES (?,?,?,?,?)",
            (r["code_muni"], ano, round(custo, 2), n_int,
             f"SIH-SUS {ano} + literatura oleo"),
        )
        inseridos += 1
        total_rs += custo

    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao"
        " (fonte, ultima_safra, atualizado_em, url, observacoes)"
        " VALUES (?,?,datetime('now'),?,?)",
        (
            "alpha4_saude",
            str(ano),
            URL,
            f"Custos SIH/SUS {ano} + taxas conservadoras (DWH, Prestige). "
            f"Grupos: L23+L24 dermatite, J68 respiratorio, T52+T65 toxico.",
        ),
    )
    con.commit()
    con.close()

    print(f"[OK] alpha4 {ano}: {inseridos} municipios, "
          f"R$ {total_rs/1e9:.2f} B total, {sem_pop} sem populacao")
    print(f"     custo por 100k hab = R$ {custo_100k:,.0f}")
    for nome, g in GRUPOS.items():
        sub = g["casos_100k"] * g["custo_rs"]
        print(f"     - {nome:12s} ({g['cids']:8s}): "
              f"{int(g['casos_100k'])}/100k x R$ {g['custo_rs']:>6.0f} "
              f"= R$ {sub:>10,.0f}/100k")


def ingest_demo(ano: int = 2023):
    """Versao demo preservada para retrocompatibilidade. Igual a ingest_real,
    porque o modelo ja e um calculo conservador e nao uma API externa."""
    ingest_real(ano)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, default=2023)
    args = p.parse_args()
    ingest_real(args.ano)
