"""
13_baixar_pop_pib_historico.py

Baixa populacao e PIB municipal por ano para todos os 5570 municipios do
Brasil, do IBGE SIDRA. Usado para contextualizar cada auto de infracao com
a realidade socioeconomica do municipio na epoca do fato.

Fontes:
  IBGE SIDRA tabela 6579 — Estimativas da populacao residente
    https://sidra.ibge.gov.br/tabela/6579
    variavel 9324 (Populacao residente estimada)
    cobertura tipica: 2001-2025 (exceto anos de censo)

  IBGE SIDRA tabela 5938 — Produto Interno Bruto dos municipios
    https://sidra.ibge.gov.br/tabela/5938
    variavel 37 (PIB a precos correntes, em mil R$)
    cobertura tipica: 2002-2023

Ambos usados no ano do fato (snapshot da epoca). Valores monetarios do PIB
serao depois corrigidos pelo IPCA no script 15.

Grava em mb_muni_socio_anual (code_muni, ano, pop, pib_rs) com chave
composta (code_muni, ano). Linhas com pop ou pib ausente ficam como NULL
naquela coluna.
"""
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

ANOS_POP = list(range(2001, 2027))  # deixa amplo, SIDRA devolve so oq existe
ANOS_PIB = list(range(2002, 2027))

POP_URL = "https://apisidra.ibge.gov.br/values/t/6579/n6/all/p/{ano}/v/9324?formato=json"
PIB_URL = "https://apisidra.ibge.gov.br/values/t/5938/n6/all/p/{ano}/v/37?formato=json"

FONTES = {
    "ibge_sidra_6579_pop": {
        "nome": "Populacao residente estimada (SIDRA 6579)",
        "orgao": "IBGE",
        "url_portal": "https://sidra.ibge.gov.br/tabela/6579",
        "uso": "Populacao do municipio no ano do fato para feature engineering do alpha1.",
        "obs": "Estimativas anuais. Anos de censo sao tratados como lacuna (NULL) e nao sao interpolados nesta etapa.",
    },
    "ibge_sidra_5938_pib": {
        "nome": "PIB municipal (SIDRA 5938)",
        "orgao": "IBGE",
        "url_portal": "https://sidra.ibge.gov.br/tabela/5938",
        "uso": "PIB do municipio no ano do fato para feature engineering do alpha1. "
               "Valores nominais; serao corrigidos pelo IPCA no script 15.",
        "obs": "Valores em mil R$ na origem, convertidos para R$ no insert.",
    },
}


def p(msg: str) -> None:
    print(msg, flush=True)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS mb_muni_socio_anual (
            code_muni TEXT NOT NULL,
            ano       INTEGER NOT NULL,
            pop       INTEGER,
            pib_rs    REAL,
            PRIMARY KEY (code_muni, ano)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_mb_socio_ano ON mb_muni_socio_anual(ano)")


def upsert(con: sqlite3.Connection, code: str, ano: int, campo: str, valor):
    con.execute(
        f"INSERT INTO mb_muni_socio_anual (code_muni, ano, {campo}) VALUES (?, ?, ?) "
        f"ON CONFLICT(code_muni, ano) DO UPDATE SET {campo}=excluded.{campo}",
        (code, ano, valor),
    )


def baixar_ano(url: str) -> list[dict]:
    for tentativa in range(3):
        try:
            r = requests.get(url, timeout=180)
            if r.status_code == 200:
                return r.json()
            p(f"  status {r.status_code}, retry {tentativa+1}/3")
        except Exception as e:
            p(f"  erro {e}, retry {tentativa+1}/3")
        time.sleep(2 ** tentativa)
    return []


def parse_valor(v: str) -> float | None:
    if v in (None, "", "-", "..", "...", "X"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def registrar_fontes(con: sqlite3.Connection, safras: dict[str, str]) -> None:
    for chave, meta in FONTES.items():
        safra = safras.get(chave, "—")
        con.execute(
            "INSERT INTO metadados_atualizacao "
            "(fonte, nome_humano, orgao, ultima_safra, atualizado_em, url, url_portal, "
            "descricao_uso, script, observacoes_metodologicas) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(fonte) DO UPDATE SET "
            "nome_humano=excluded.nome_humano, orgao=excluded.orgao, "
            "ultima_safra=excluded.ultima_safra, atualizado_em=excluded.atualizado_em, "
            "url=excluded.url, url_portal=excluded.url_portal, "
            "descricao_uso=excluded.descricao_uso, script=excluded.script, "
            "observacoes_metodologicas=excluded.observacoes_metodologicas",
            (
                chave, meta["nome"], meta["orgao"], safra,
                datetime.utcnow().isoformat(timespec="seconds"),
                POP_URL if "pop" in chave else PIB_URL,
                meta["url_portal"], meta["uso"],
                "scripts/13_baixar_pop_pib_historico.py",
                meta["obs"],
            ),
        )


def main():
    con = sqlite3.connect(DB)
    ensure_table(con)

    anos_pop_vistos: set[int] = set()
    anos_pib_vistos: set[int] = set()

    p("fase 1/2: populacao (SIDRA 6579)")
    for ano in ANOS_POP:
        dados = baixar_ano(POP_URL.format(ano=ano))
        if not dados or len(dados) <= 1:
            p(f"  [{ano}] sem dados, pulando")
            continue
        n_ok = 0
        for row in dados[1:]:
            code = row.get("D1C", "").strip()
            val = parse_valor(row.get("V"))
            if len(code) != 7 or val is None:
                continue
            upsert(con, code, ano, "pop", int(val))
            n_ok += 1
        con.commit()
        if n_ok:
            anos_pop_vistos.add(ano)
            p(f"  [{ano}] {n_ok} munis")

    p("")
    p("fase 2/2: PIB (SIDRA 5938)")
    for ano in ANOS_PIB:
        dados = baixar_ano(PIB_URL.format(ano=ano))
        if not dados or len(dados) <= 1:
            p(f"  [{ano}] sem dados, pulando")
            continue
        n_ok = 0
        for row in dados[1:]:
            code = row.get("D1C", "").strip()
            val = parse_valor(row.get("V"))
            if len(code) != 7 or val is None:
                continue
            # tabela 5938 vem em "Mil Reais"
            upsert(con, code, ano, "pib_rs", val * 1000.0)
            n_ok += 1
        con.commit()
        if n_ok:
            anos_pib_vistos.add(ano)
            p(f"  [{ano}] {n_ok} munis")

    def fmt(anos: set[int]) -> str:
        if not anos:
            return "—"
        s = sorted(anos)
        return f"{s[0]}-{s[-1]}"

    registrar_fontes(con, {
        "ibge_sidra_6579_pop": fmt(anos_pop_vistos),
        "ibge_sidra_5938_pib": fmt(anos_pib_vistos),
    })
    con.commit()

    total = con.execute("SELECT COUNT(*) FROM mb_muni_socio_anual").fetchone()[0]
    com_pop = con.execute(
        "SELECT COUNT(*) FROM mb_muni_socio_anual WHERE pop IS NOT NULL"
    ).fetchone()[0]
    com_pib = con.execute(
        "SELECT COUNT(*) FROM mb_muni_socio_anual WHERE pib_rs IS NOT NULL"
    ).fetchone()[0]
    con.close()
    p("")
    p(f"[OK] mb_muni_socio_anual: {total} linhas totais")
    p(f"     com pop : {com_pop}")
    p(f"     com pib : {com_pib}")
    p(f"     anos pop: {fmt(anos_pop_vistos)}")
    p(f"     anos pib: {fmt(anos_pib_vistos)}")


if __name__ == "__main__":
    main()
