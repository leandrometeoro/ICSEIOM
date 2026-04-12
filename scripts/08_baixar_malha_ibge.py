"""
08_baixar_malha_ibge.py
Baixa a malha municipal completa do Brasil (5570 municipios) via API do IBGE.

Fontes:
- Malhas:      https://servicodados.ibge.gov.br/api/v3/malhas/estados/{UF}
- Localidades: https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/municipios

Gera:
- app/static/data/municipios_br.geojson   (FeatureCollection com todos os municipios)
- Popula a tabela municipios_brasil (nome, uf, regiao). is_costeiro = 0 por padrao
  (o script 09 marca depois).

Uso:
    python scripts/08_baixar_malha_ibge.py
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
OUT_DIR = ROOT / "app" / "static" / "data"
OUT_FILE = OUT_DIR / "municipios_br.geojson"

UFS = [
    ("AC", "Norte"), ("AP", "Norte"), ("AM", "Norte"), ("PA", "Norte"),
    ("RO", "Norte"), ("RR", "Norte"), ("TO", "Norte"),
    ("AL", "Nordeste"), ("BA", "Nordeste"), ("CE", "Nordeste"),
    ("MA", "Nordeste"), ("PB", "Nordeste"), ("PE", "Nordeste"),
    ("PI", "Nordeste"), ("RN", "Nordeste"), ("SE", "Nordeste"),
    ("DF", "Centro-Oeste"), ("GO", "Centro-Oeste"),
    ("MT", "Centro-Oeste"), ("MS", "Centro-Oeste"),
    ("ES", "Sudeste"), ("MG", "Sudeste"), ("RJ", "Sudeste"), ("SP", "Sudeste"),
    ("PR", "Sul"), ("RS", "Sul"), ("SC", "Sul"),
]

MALHA_URL = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}"
    "?formato=application/vnd.geo+json&qualidade=intermediaria&intrarregiao=municipio"
)
LOCAL_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"


def baixar_uf(uf: str, regiao: str) -> list[dict]:
    print(f"  [{uf}] baixando malha...", flush=True)
    r = requests.get(MALHA_URL.format(uf=uf), timeout=180)
    r.raise_for_status()
    gj = r.json()

    print(f"  [{uf}] baixando nomes...", flush=True)
    r2 = requests.get(LOCAL_URL.format(uf=uf), timeout=60)
    r2.raise_for_status()
    nomes = {str(m["id"]): m["nome"] for m in r2.json()}

    feats = gj.get("features", [])
    out = []
    for f in feats:
        props = f.get("properties", {}) or {}
        code = str(props.get("codarea") or props.get("CD_MUN") or "").strip()
        if len(code) != 7:
            continue
        nome = nomes.get(code, props.get("nome") or "")
        f["properties"] = {
            "code_muni": code,
            "nome": nome,
            "uf": uf,
            "regiao": regiao,
            "is_costeiro": 0,
        }
        out.append(f)
    print(f"  [{uf}] {len(out)} municipios", flush=True)
    return out


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_feats: list[dict] = []
    for uf, regiao in UFS:
        tries = 0
        while True:
            try:
                all_feats.extend(baixar_uf(uf, regiao))
                break
            except Exception as e:
                tries += 1
                if tries >= 3:
                    print(f"  [{uf}] FALHOU: {e}", file=sys.stderr)
                    raise
                print(f"  [{uf}] erro '{e}', retry {tries}/3 em 3s...", flush=True)
                time.sleep(3)

    fc = {"type": "FeatureCollection", "features": all_feats}
    OUT_FILE.write_text(json.dumps(fc, ensure_ascii=False))
    size_mb = OUT_FILE.stat().st_size / 1024 / 1024
    print(f"[OK] {OUT_FILE} ({len(all_feats)} features, {size_mb:.1f} MB)")

    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")
    rows = [
        (f["properties"]["code_muni"], f["properties"]["nome"],
         f["properties"]["uf"], f["properties"]["regiao"])
        for f in all_feats
    ]
    con.executemany(
        "INSERT INTO municipios_brasil (code_muni, nome, uf, regiao, is_costeiro) "
        "VALUES (?, ?, ?, ?, 0) "
        "ON CONFLICT(code_muni) DO UPDATE SET "
        "nome=excluded.nome, uf=excluded.uf, regiao=excluded.regiao",
        rows,
    )
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM municipios_brasil").fetchone()[0]
    con.close()
    print(f"[OK] municipios_brasil: {n} linhas")


if __name__ == "__main__":
    main()
