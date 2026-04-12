"""
02_ingest_alpha1_ibama.py   —   α₁ multas ambientais (Lei 9.966/2000)

Fonte: IBAMA SICAFI / Dados Abertos.
URL:   https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao

Fluxo:
  1. Baixar o CSV consolidado.
  2. Filtrar por descricao contendo 'oleo', 'hidrocarboneto', 'derramamento'.
  3. Agrupar por municipio (codigo IBGE) e ano, somando valor_consolidado.
  4. Gravar em alpha1_multa_ambiental.

Nesta versao de demonstracao, popula o banco com valores plausiveis
por municipio (derivados de um modelo simples proporcional a populacao
e a probabilidade historica de incidentes), para o exercicio 2024.
"""
import sqlite3
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

URL = "https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao"

def ingest_real(ano: int):
    """Stub para ingestao real. Ativar em producao."""
    # import requests, pandas as pd, io
    # r = requests.get(URL_CSV)
    # df = pd.read_csv(io.BytesIO(r.content), sep=';')
    # df = df[df['DES_TIPO_INFRACAO'].str.contains('oleo|hidrocarb|derram', case=False, na=False)]
    # df = df[df['DAT_AUTO_INFRACAO'].str.startswith(str(ano))]
    # agg = df.groupby('COD_MUNICIPIO')['VAL_AUTO_INFRACAO'].agg(['sum','count']).reset_index()
    # return agg
    raise NotImplementedError("Ative em producao.")

def ingest_demo(ano: int = 2024):
    """Popula valores de demonstracao baseados em coeficientes por regiao."""
    con = sqlite3.connect(DB)
    cur = con.cursor()
    muns = cur.execute(
        "SELECT code_muni, uf, regiao, pop_2022 FROM municipios_costeiros"
    ).fetchall()
    # coeficientes por regiao (R$ per capita por ano, ordem de grandeza 2023-2024)
    base = {"Norte": 0.18, "Nordeste": 0.42, "Sudeste": 1.25, "Sul": 0.35}
    cur.execute("DELETE FROM alpha1_multa_ambiental WHERE ano = ?", (ano,))
    for code, uf, regiao, pop in muns:
        pop = pop or 1
        # peso adicional para hubs portuarios / polos petroliferos
        extra = 1.0
        if code in ("3301009", "3302270", "3550308", "3548500", "2611606",
                    "2607901", "3304557", "4318309"):
            extra = 4.5
        valor = pop * base.get(regiao, 0.5) * extra
        n_autos = max(1, int(valor / 15000))
        cur.execute(
            """INSERT INTO alpha1_multa_ambiental
               (code_muni, ano, valor_rs, n_autos, fonte)
               VALUES (?,?,?,?,?)""",
            (code, ano, round(valor, 2), n_autos, "demo-modelo"),
        )
    cur.execute(
        """INSERT OR REPLACE INTO metadados_atualizacao
           (fonte, ultima_safra, atualizado_em, url, observacoes)
           VALUES (?,?,datetime('now'),?,?)""",
        ("alpha1_ibama", str(ano), URL, "valores de demonstracao"),
    )
    con.commit()
    n = cur.execute(
        "SELECT COUNT(*) FROM alpha1_multa_ambiental WHERE ano=?", (ano,)
    ).fetchone()[0]
    total = cur.execute(
        "SELECT SUM(valor_rs) FROM alpha1_multa_ambiental WHERE ano=?", (ano,)
    ).fetchone()[0]
    con.close()
    print(f"[OK] α₁ {ano}: {n} municipios, total R$ {total:,.2f}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, default=2024)
    p.add_argument("--real", action="store_true", help="ingerir da fonte real")
    a = p.parse_args()
    (ingest_real if a.real else ingest_demo)(a.ano)
