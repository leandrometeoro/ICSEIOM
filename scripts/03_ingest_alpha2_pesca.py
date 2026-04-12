"""
03_ingest_alpha2_pesca.py   —   α₂ producao pesqueira preservada

Fontes:
- IBGE SIDRA — Producao da Pecuaria Municipal, tabela 3940 (aquicultura).
  API: https://apisidra.ibge.gov.br/values/t/3940
- MAPA RGP (hiato 2012-2020).
- PMAP-BS (convenios ANP/Petrobras/UFs).

Demo: valores proporcionais a populacao costeira e fator regional.
"""
import sqlite3, argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
URL = "https://apisidra.ibge.gov.br/values/t/3940"

def ingest_demo(ano=2024):
    con = sqlite3.connect(DB); cur = con.cursor()
    muns = cur.execute(
        "SELECT code_muni, regiao, pop_2022 FROM municipios_costeiros"
    ).fetchall()
    base = {"Norte": 4.2, "Nordeste": 2.8, "Sudeste": 1.1, "Sul": 3.5}  # R$/per capita (pesca artesanal+indust)
    cur.execute("DELETE FROM alpha2_pesca WHERE ano=?", (ano,))
    for code, regiao, pop in muns:
        pop = pop or 1
        valor = pop * base.get(regiao, 2.0) * 1000  # * 1000 p/ escala anual municipal
        ton = valor / 18_000
        cur.execute(
            "INSERT INTO alpha2_pesca (code_muni, ano, valor_rs, toneladas, fonte) VALUES (?,?,?,?,?)",
            (code, ano, round(valor, 2), round(ton, 1), "demo-modelo"),
        )
    cur.execute(
        """INSERT OR REPLACE INTO metadados_atualizacao
           (fonte, ultima_safra, atualizado_em, url, observacoes)
           VALUES (?,?,datetime('now'),?,?)""",
        ("alpha2_pesca", str(ano), URL, "demo"),
    )
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM alpha2_pesca WHERE ano=?", (ano,)).fetchone()[0]
    con.close()
    print(f"[OK] α₂ {ano}: {n} municipios")

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--ano", type=int, default=2024)
    ingest_demo(p.parse_args().ano)
