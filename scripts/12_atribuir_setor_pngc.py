"""
12_atribuir_setor_pngc.py

Atribui cada municipio costeiro a um dos 5 setores do PNGC (Plano Nacional de
Gerenciamento Costeiro, Decreto 5.300/2004) usado como estrato geografico para
o clustering e a regressao de alpha1.

Esta versao usa mapeamento por UF — aproximacao defensavel para o nivel de
resolucao que precisamos (estrato macrorregional). Refinamento possivel:
usar a lista municipal oficial do MMA/IBGE PNGC 2021 (quando a tabela for
liberada em formato estruturado) para corrigir os municipios fronteirasos
(Sul da BA, Norte do RJ, etc.) que podem pertencer a setor diferente do UF.

Setores:
  1 Norte           — AP, PA, MA
  2 Nordeste        — PI, CE, RN, PB, PE, AL, SE, BA
  3 Leste           — ES
  4 Sudeste         — RJ, SP
  5 Sul             — PR, SC, RS

Adiciona coluna setor_pngc (INTEGER 1-5) em municipios_brasil.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

FONTE_CHAVE = "pngc_setores_decreto_5300"

UF_SETOR: dict[str, int] = {
    # Norte
    "AP": 1, "PA": 1, "MA": 1,
    # Nordeste
    "PI": 2, "CE": 2, "RN": 2, "PB": 2, "PE": 2, "AL": 2, "SE": 2, "BA": 2,
    # Leste
    "ES": 3,
    # Sudeste
    "RJ": 4, "SP": 4,
    # Sul
    "PR": 5, "SC": 5, "RS": 5,
}

SETOR_NOME = {
    1: "Norte",
    2: "Nordeste",
    3: "Leste",
    4: "Sudeste",
    5: "Sul",
}


def p(msg: str) -> None:
    print(msg, flush=True)


def ensure_column(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(municipios_brasil)")}
    if "setor_pngc" not in cols:
        con.execute("ALTER TABLE municipios_brasil ADD COLUMN setor_pngc INTEGER")
        p("  coluna setor_pngc adicionada em municipios_brasil")


def registrar_fonte(con: sqlite3.Connection, n_atribuidos: int) -> None:
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
            "Setores PNGC (Decreto 5.300/2004)",
            "MMA",
            "2004",
            datetime.utcnow().isoformat(timespec="seconds"),
            "https://www.planalto.gov.br/ccivil_03/_ato2004-2006/2004/decreto/d5300.htm",
            "https://www.gov.br/mma/pt-br/assuntos/gestaoterritorial/gerenciamento-costeiro",
            "Estrato geografico macrorregional para clustering e regressao de alpha1. "
            "5 setores: Norte, Nordeste, Leste, Sudeste, Sul.",
            "scripts/12_atribuir_setor_pngc.py",
            f"Mapeamento UF-level (aproximacao). {n_atribuidos} munis atribuidos.",
        ),
    )


def main():
    con = sqlite3.connect(DB)
    ensure_column(con)

    munis = con.execute(
        "SELECT code_muni, uf FROM municipios_brasil"
    ).fetchall()
    p(f"municipios carregados: {len(munis)}")

    n_atribuidos = 0
    n_ignorados = 0
    for code, uf in munis:
        setor = UF_SETOR.get(uf)
        if setor is None:
            n_ignorados += 1
            continue
        con.execute(
            "UPDATE municipios_brasil SET setor_pngc = ? WHERE code_muni = ?",
            (setor, code),
        )
        n_atribuidos += 1

    distrib = con.execute(
        "SELECT setor_pngc, COUNT(*) FROM municipios_brasil "
        "WHERE is_costeiro = 1 AND setor_pngc IS NOT NULL "
        "GROUP BY setor_pngc ORDER BY setor_pngc"
    ).fetchall()

    registrar_fonte(con, n_atribuidos)
    con.commit()
    con.close()

    p("")
    p(f"[OK] setor_pngc atribuido a {n_atribuidos} munis ({n_ignorados} sem UF mapeada)")
    p("distribuicao nos costeiros:")
    for setor, n in distrib:
        p(f"  setor {setor} {SETOR_NOME[setor]:<10}: {n:4d} munis")


if __name__ == "__main__":
    main()
