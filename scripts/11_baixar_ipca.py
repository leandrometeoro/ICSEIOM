"""
11_baixar_ipca.py

Baixa a serie mensal do IPCA (BCB SGS serie 433) e grava em ipca_mensal.
Usada para corrigir multas, PIB municipal e qualquer outro valor monetario
historico para reais do mes corrente.

Fonte: Banco Central do Brasil — Sistema Gerenciador de Series Temporais
  https://www3.bcb.gov.br/sgspub/
  API aberta (JSON): https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json

A serie 433 e o IPCA mensal (variacao % m/m) do IBGE publicada pelo BCB.
Para corrigir um valor do mes X para o mes Y, multiplica-se pelo fator
composto (produtorio de (1 + ipca_mensal/100) entre X+1 e Y).
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
FONTE_CHAVE = "bcb_sgs_433_ipca"


def p(msg: str) -> None:
    print(msg, flush=True)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS ipca_mensal (
            ano       INTEGER NOT NULL,
            mes       INTEGER NOT NULL,
            valor_pct REAL NOT NULL,
            PRIMARY KEY (ano, mes)
        )
    """)


def registrar_fonte(con: sqlite3.Connection, safra: str, n_linhas: int) -> None:
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
            FONTE_CHAVE,
            "IPCA mensal (BCB SGS 433)",
            "BCB / IBGE",
            safra,
            datetime.utcnow().isoformat(timespec="seconds"),
            URL,
            "https://www3.bcb.gov.br/sgspub/",
            "Corrige multas, PIB e demais valores monetarios historicos para reais "
            "do mes corrente, via fator composto mensal.",
            "scripts/11_baixar_ipca.py",
            f"Serie oficial IBGE redistribuida pelo BCB. {n_linhas} meses carregados.",
        ),
    )


def main():
    p(f"baixando IPCA mensal de {URL}...")
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    dados = r.json()
    p(f"  {len(dados)} registros recebidos")

    linhas = []
    for d in dados:
        # formato: {"data": "01/01/1980", "valor": "6.62"}
        dt = datetime.strptime(d["data"], "%d/%m/%Y")
        linhas.append((dt.year, dt.month, float(d["valor"])))

    linhas.sort()
    if not linhas:
        p("ERRO: serie vazia")
        sys.exit(1)

    con = sqlite3.connect(DB)
    ensure_table(con)
    con.execute("DELETE FROM ipca_mensal")
    con.executemany(
        "INSERT INTO ipca_mensal (ano, mes, valor_pct) VALUES (?, ?, ?)",
        linhas,
    )
    primeiro = f"{linhas[0][0]:04d}-{linhas[0][1]:02d}"
    ultimo = f"{linhas[-1][0]:04d}-{linhas[-1][1]:02d}"
    registrar_fonte(con, f"{primeiro} a {ultimo}", len(linhas))
    con.commit()
    con.close()
    p(f"[OK] ipca_mensal: {len(linhas)} linhas, {primeiro} a {ultimo}")


if __name__ == "__main__":
    main()
