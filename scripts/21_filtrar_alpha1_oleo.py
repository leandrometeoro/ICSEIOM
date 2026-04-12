"""
21_filtrar_alpha1_oleo.py

Marca quais autos de mb_alpha1_autos sao efetivamente relacionados a oleo/
hidrocarbonetos e reconstroi mb_alpha1_multa.

Regra (dois trilhos):

  A) Artigo especifico de oleo -> RELEVANTE independente da descricao:
     - Lei 9966/00 artigo em {3, 4-22, 32} (tipifica conduta operacional
       de oleo). EXCLUIDOS: 25 (catch-all "descumprir esta Lei"),
       26 (penalidades), 27 (classificacao de gravidade), 70 (fora do
       escopo).
     - Decreto 4136/2002 qualquer artigo (decreto inteiro regulamenta
       a 9966, e portanto sobre oleo).

  B) Artigo generico (nao tipifica oleo) -> RELEVANTE somente se a
     descricao bater na whitelist de oleo/hidrocarbonetos:
     - Decreto 6514/2008 (decreto geral de sancoes ambientais)
     - Lei 9605/98 (crimes ambientais gerais)
     - Autos sem norma ou com norma nao classificada

Saida:
  - mb_alpha1_autos.relevante_oleo (0/1)
  - mb_alpha1_multa reconstruida
  - mb_alpha1_multa_bruto backup
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

WHITELIST = re.compile(
    r"\bóleo\b|\bóleos\b|hidrocarb|petról|petrol|combustí|gasolin|diesel|"
    r"oleoduto|lubrificant|\bGLP\b|liquefeito de petról|bunker",
    re.IGNORECASE,
)
BLACKLIST = re.compile(
    r"óleos? vegetais?|óleo vegetal|óleo essencial|"
    r"cadastro técnico federal|sem inscrição no cadastro|"
    r"falta de entrega.*relatório anual|deixar de inscrever-se",
    re.IGNORECASE,
)

# Artigos que TIPIFICAM descarga/derrame de oleo, lidos do texto oficial.
#
# Lei 9966/00: so os arts 15, 16, 17 proibem descarga em aguas sob jurisdicao
# nacional (cat A, cat B/C/D, oleo+mistura+lixo respectivamente). O restante
# da 9966 e preventivo (plano de emergencia, auditoria bienal, livro de
# oleo, etc) ou estrutural (definicoes, competencia).
#
# Dec 4136/02: os arts 29 a 45 formam as Subsecoes VI a XVII "Das Infracoes
# Relativas a Descarga". Excluidas subsecoes de esgoto sanitario (34-35) e
# plasticos (40-41), que nao sao oleo.
LEI_9966_ART_OLEO = {"15", "16", "17"}
DEC_4136_ART_OLEO = {
    "29", "30", "31", "32", "33",   # descarga de substancias A, B, C, D
    "36", "37",                     # descarga de oleo/misturas oleosas/lixo
    "38", "39",                     # descarte de agua de processo/producao
    "42", "43", "44", "45",         # excecoes sem comprovacao / dano constatado
}


def norm_artigo(a: str | None) -> str:
    """Normaliza '17º' -> '17', '1°' -> '1', 'creto27' -> 'creto27' (lixo)."""
    if not a:
        return ""
    s = re.sub(r"[°º]", "", a).strip()
    return s


def p(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cols = {r["name"] for r in cur.execute("PRAGMA table_info(mb_alpha1_autos)")}
    if "relevante_oleo" not in cols:
        cur.execute("ALTER TABLE mb_alpha1_autos ADD COLUMN relevante_oleo INTEGER DEFAULT 0")
        p("coluna relevante_oleo criada")

    cur.execute("UPDATE mb_alpha1_autos SET relevante_oleo = 0")

    rows = cur.execute(
        "SELECT seq_auto, tp_norma, nu_norma, artigo, des_infracao "
        "FROM mb_alpha1_autos"
    ).fetchall()

    keep_A: list[str] = []  # artigo oleo-especifico
    keep_B: list[str] = []  # artigo generico + descricao bate whitelist

    for r in rows:
        tpn = (r["tp_norma"] or "").strip()
        nun = (r["nu_norma"] or "").strip()
        art = norm_artigo(r["artigo"])
        des = r["des_infracao"] or ""
        seq = r["seq_auto"]

        artigo_especifico = False
        if tpn == "Lei" and nun == "9966/00":
            if art in LEI_9966_ART_OLEO:
                artigo_especifico = True
        elif tpn == "Decreto" and nun == "4136/2002":
            if art in DEC_4136_ART_OLEO:
                artigo_especifico = True

        if artigo_especifico:
            keep_A.append(seq)
        elif WHITELIST.search(des) and not BLACKLIST.search(des):
            keep_B.append(seq)

    def marcar(lista: list[str]) -> None:
        if not lista:
            return
        for i in range(0, len(lista), 900):
            chunk = lista[i:i + 900]
            ph = ",".join("?" * len(chunk))
            cur.execute(
                f"UPDATE mb_alpha1_autos SET relevante_oleo = 1 WHERE seq_auto IN ({ph})",
                chunk,
            )

    marcar(keep_A)
    marcar(keep_B)

    p(f"trilho A (artigo oleo-especifico): {len(keep_A)} autos")
    p(f"trilho B (artigo generico + descricao oleo): {len(keep_B)} autos")

    total_rel = cur.execute(
        "SELECT COUNT(*) c, ROUND(SUM(valor_rs), 2) t FROM mb_alpha1_autos WHERE relevante_oleo = 1"
    ).fetchone()
    total_all = cur.execute(
        "SELECT COUNT(*) c, ROUND(SUM(valor_rs), 2) t FROM mb_alpha1_autos"
    ).fetchone()
    p(f"relevantes: {total_rel['c']} autos, R$ {total_rel['t']:,.2f}")
    p(f"universo:   {total_all['c']} autos, R$ {total_all['t']:,.2f}")

    # Breakdown
    p("\nbreakdown por norma/artigo:")
    bd = cur.execute(
        "SELECT tp_norma, nu_norma, artigo, COUNT(*) c, "
        "ROUND(SUM(valor_rs),0) t FROM mb_alpha1_autos "
        "WHERE relevante_oleo = 1 "
        "GROUP BY tp_norma, nu_norma, artigo ORDER BY c DESC LIMIT 30"
    ).fetchall()
    for r in bd:
        p(f"  {r['tp_norma'] or '-'} {r['nu_norma'] or '-'} art {r['artigo'] or '-'}: "
          f"{r['c']} autos R$ {r['t']:,.0f}")

    # Backup + rebuild mb_alpha1_multa
    cur.execute("DROP TABLE IF EXISTS mb_alpha1_multa_bruto")
    cur.execute("CREATE TABLE mb_alpha1_multa_bruto AS SELECT * FROM mb_alpha1_multa")
    p("\nbackup mb_alpha1_multa_bruto criado")

    cur.execute("DELETE FROM mb_alpha1_multa")
    cur.execute(
        "INSERT INTO mb_alpha1_multa (code_muni, ano, valor_rs, n_autos, fonte) "
        "SELECT code_muni, ano, SUM(valor_rs), COUNT(*), "
        "'IBAMA SIFISC (filtro óleo: artigo específico OU descrição)' "
        "FROM mb_alpha1_autos WHERE relevante_oleo = 1 "
        "GROUP BY code_muni, ano"
    )
    n_rows = cur.execute("SELECT COUNT(*) c FROM mb_alpha1_multa").fetchone()["c"]
    tot = cur.execute("SELECT ROUND(SUM(valor_rs), 2) s FROM mb_alpha1_multa").fetchone()["s"]
    p(f"mb_alpha1_multa reconstruida: {n_rows} linhas, R$ {tot:,.2f}")

    con.commit()
    con.close()
    p("ok")


if __name__ == "__main__":
    main()
