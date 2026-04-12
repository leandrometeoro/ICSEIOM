"""
14_marcar_infra_oleo.py

Marca municipios costeiros que hospedam infraestrutura de cadeia do petroleo.
Usado como feature binaria para o clustering e a regressao de alpha1.

Quatro flags independentes:
  - tem_refinaria    : refinaria da Petrobras ou privada
  - tem_terminal     : terminal aquaviario de derivados (Petrobras/Transpetro ou TUP)
  - tem_duto         : trecho de oleoduto relevante (nao apenas gasoduto)
  - tem_campo_eep    : adjacente a campo de exploracao & producao offshore ativo

Fontes da lista curada:
  - ANP Anuario Estatistico 2024 (Capitulos 2, 3 e 4)
    https://www.gov.br/anp/pt-br/centrais-de-conteudo/publicacoes/anuario-estatistico
  - Petrobras / Transpetro — pagina institucional de Refino e Logistica
    https://petrobras.com.br/nossas-atividades/refino
    https://transpetro.com.br/pt_br/nossos-negocios/terminais-e-oleodutos.html
  - ANTAQ — Estatistico Aquaviario (lista de portos organizados e TUPs com
    movimentacao de granel liquido / petroleo e derivados)
    https://www.gov.br/antaq/pt-br/central-de-conteudos/estatisticos

PLACEHOLDER: a lista e curada manualmente e pode estar incompleta em munis
secundarios (TUPs pequenos, dutos de distribuicao interna, etc.). Refinamento
possivel: parser automatizado dos CSVs abertos da ANP e ANTAQ, cruzando por
codigo IBGE. Ver observacoes_metodologicas na metodologia.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

FONTE_CHAVE = "anp_antaq_infra_oleo"

# Codigos IBGE 7 digitos. Lista curada a partir das fontes acima.
# (code_muni, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep, descricao)
REGISTROS: list[tuple[str, int, int, int, int, str]] = [
    # ---- Refinarias Petrobras / privadas ----
    ("3304557", 1, 1, 1, 1, "Rio de Janeiro - RJ (REDUC em Duque de Caxias proxima; terminal Ilha Redonda)"),
    ("3301702", 1, 1, 1, 0, "Duque de Caxias - RJ (REDUC - Refinaria Duque de Caxias)"),
    ("3549904", 1, 1, 1, 1, "Sao Sebastiao - SP (Terminal Almirante Barroso + duto OSBAT)"),
    ("3547809", 1, 1, 1, 1, "Santos - SP (Porto de Santos + Alemoa)"),
    ("3513009", 1, 1, 1, 0, "Cubatao - SP (RPBC - Refinaria Presidente Bernardes)"),
    ("3541000", 1, 0, 1, 0, "Paulinia - SP (REPLAN - maior refinaria do pais)"),
    ("3552205", 1, 0, 1, 0, "Sao Jose dos Campos - SP (REVAP - Refinaria Henrique Lage)"),
    ("3108909", 1, 0, 1, 0, "Betim - MG (REGAP - Refinaria Gabriel Passos; inland mas marca UF)"),
    ("2927408", 1, 1, 1, 0, "Sao Francisco do Conde - BA (RLAM/Mataripe - Refinaria Landulpho Alves)"),
    ("4314902", 1, 1, 1, 0, "Rio Grande - RS (Refinaria Riograndense; porto)"),
    ("4306502", 1, 0, 1, 0, "Canoas - RS (REFAP - Refinaria Alberto Pasqualini)"),
    ("4125506", 1, 1, 1, 0, "Araucaria - PR (REPAR - Refinaria Presidente Getulio Vargas)"),
    ("1302603", 1, 1, 0, 0, "Manaus - AM (REMAN - nao costeiro, fora de escopo mas marcado)"),
    ("2304400", 1, 1, 0, 0, "Fortaleza - CE (LUBNOR - Lubrificantes e Derivados Nordeste)"),

    # ---- Grandes terminais aquaviarios de derivados ----
    ("2111300", 0, 1, 1, 0, "Sao Luis / Itaqui - MA (Terminal de Ponta da Madeira, TUP petroleo)"),
    ("2607901", 0, 1, 0, 0, "Ipojuca - PE (Suape - terminal de combustiveis)"),
    ("2304202", 0, 1, 0, 0, "Caucaia - CE (Pecem - terminal petroleo)"),
    ("2704302", 0, 1, 0, 0, "Maceio - AL (Terminal Maceio/Transpetro)"),
    ("2800308", 0, 1, 0, 0, "Aracaju - SE (Terminal Aracaju/Transpetro)"),
    ("2927101", 0, 1, 0, 0, "Salvador - BA (Terminal Madre de Deus proximo)"),
    ("2919207", 0, 1, 0, 0, "Madre de Deus - BA (Terminal Aquaviario Madre de Deus - TEMADRE)"),
    ("2914802", 0, 1, 0, 0, "Ilheus - BA (Porto de Ilheus)"),
    ("3205200", 0, 1, 1, 1, "Vitoria - ES (Porto de Tubarao / Barra do Riacho proximo)"),
    ("3201506", 0, 1, 1, 0, "Aracruz - ES (Terminal Norte Capixaba / Barra do Riacho - TNC)"),
    ("3303302", 0, 1, 0, 1, "Niteroi - RJ (proximo a Baia de Guanabara / terminais)"),
    ("3300704", 0, 1, 0, 1, "Arraial do Cabo - RJ (Terminal Forno / Bacia de Campos)"),
    ("3302403", 0, 1, 1, 1, "Macae - RJ (base logistica offshore Bacia de Campos)"),
    ("3304151", 0, 1, 0, 1, "Rio das Ostras - RJ (proximo a Macae/Bacia de Campos)"),
    ("4115200", 0, 1, 1, 0, "Paranagua - PR (Porto de Paranagua - granel liquido)"),
    ("4205407", 0, 1, 0, 0, "Itajai - SC (Porto de Itajai)"),
    ("4204202", 0, 1, 0, 0, "Imbituba - SC (Porto de Imbituba)"),

    # ---- Municipios adjacentes a campos de E&P offshore ativos ----
    # Bacia de Campos (RJ) - nucleo producao petroleo nacional
    ("3300506", 0, 0, 0, 1, "Araruama - RJ (adjacente Campos)"),
    ("3300803", 0, 0, 0, 1, "Armacao dos Buzios - RJ (adjacente Campos)"),
    ("3301009", 0, 0, 0, 1, "Cabo Frio - RJ (adjacente Campos)"),
    ("3301306", 0, 0, 0, 1, "Campos dos Goytacazes - RJ (adjacente Campos)"),
    ("3302254", 0, 0, 0, 1, "Quissama - RJ (adjacente Campos)"),
    ("3302270", 0, 0, 0, 1, "Sao Joao da Barra - RJ (Porto do Acu)"),
    # Bacia de Santos (SP/RJ) - Pre-sal
    ("3520400", 0, 0, 0, 1, "Ilhabela - SP (adjacente Bacia de Santos)"),
    ("3510609", 0, 0, 0, 1, "Caraguatatuba - SP (gasoduto + adjacente)"),
    # Bacia de Camamu-Almada (BA)
    ("2910800", 0, 0, 0, 1, "Ituberá - BA (adjacente Camamu)"),
    # Bacia de Sergipe-Alagoas
    ("2704609", 0, 0, 0, 1, "Pilar - AL (onshore mas bacia SE-AL)"),
    # Bacia Potiguar (RN/CE)
    ("2404309", 0, 0, 0, 1, "Macau - RN (Bacia Potiguar)"),
    ("2400307", 0, 0, 0, 1, "Areia Branca - RN (Bacia Potiguar)"),
    ("2404200", 0, 0, 0, 1, "Guamare - RN (terminal Guamare + Potiguar)"),
    # Foz do Amazonas / Para
    ("1508126", 0, 0, 0, 1, "Porto de Moz - PA (Foz do Amazonas exploratoria)"),
    ("1500602", 0, 0, 0, 1, "Almeirim - PA (Foz do Amazonas exploratoria)"),
]


def p(msg: str) -> None:
    print(msg, flush=True)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS mb_infra_oleo (
            code_muni     TEXT PRIMARY KEY,
            tem_refinaria INTEGER NOT NULL DEFAULT 0,
            tem_terminal  INTEGER NOT NULL DEFAULT 0,
            tem_duto      INTEGER NOT NULL DEFAULT 0,
            tem_campo_eep INTEGER NOT NULL DEFAULT 0,
            tem_infra     INTEGER GENERATED ALWAYS AS (
                CASE WHEN tem_refinaria + tem_terminal + tem_duto + tem_campo_eep > 0
                     THEN 1 ELSE 0 END
            ) VIRTUAL,
            descricao     TEXT
        )
    """)


def registrar_fonte(con: sqlite3.Connection, n: int) -> None:
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
            "Infraestrutura de oleo (refinarias, terminais, dutos, campos E&P)",
            "ANP / ANTAQ / Petrobras (curadoria)",
            "2024",
            datetime.utcnow().isoformat(timespec="seconds"),
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/publicacoes/anuario-estatistico",
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos",
            "Feature binaria de exposicao a cadeia do petroleo, usada no clustering "
            "por setor PNGC e na regressao de alpha1.",
            "scripts/14_marcar_infra_oleo.py",
            f"Lista curada manualmente a partir do Anuario ANP 2024, portal Transpetro "
            f"e Estatistico ANTAQ. {n} municipios marcados. Placeholder ate parser "
            f"automatizado dos CSVs abertos da ANP ser implementado.",
        ),
    )


def main():
    con = sqlite3.connect(DB)
    ensure_table(con)
    con.execute("DELETE FROM mb_infra_oleo")
    con.executemany(
        "INSERT INTO mb_infra_oleo "
        "(code_muni, tem_refinaria, tem_terminal, tem_duto, tem_campo_eep, descricao) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        REGISTROS,
    )
    n = con.execute("SELECT COUNT(*) FROM mb_infra_oleo").fetchone()[0]

    # cruzamento com costeiros para validar
    stats = con.execute("""
        SELECT
            SUM(CASE WHEN i.tem_refinaria=1 THEN 1 ELSE 0 END) ref,
            SUM(CASE WHEN i.tem_terminal=1 THEN 1 ELSE 0 END) term,
            SUM(CASE WHEN i.tem_duto=1 THEN 1 ELSE 0 END) duto,
            SUM(CASE WHEN i.tem_campo_eep=1 THEN 1 ELSE 0 END) eep,
            COUNT(*) total_costeiros_com_infra
        FROM mb_infra_oleo i
        JOIN municipios_brasil m ON m.code_muni = i.code_muni
        WHERE m.is_costeiro = 1
    """).fetchone()

    registrar_fonte(con, n)
    con.commit()
    con.close()
    p(f"[OK] mb_infra_oleo: {n} municipios marcados")
    p(f"     costeiros com infra: {stats[4]} (ref={stats[0]} term={stats[1]} duto={stats[2]} eep={stats[3]})")


if __name__ == "__main__":
    main()
