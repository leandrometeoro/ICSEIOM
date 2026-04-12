"""
09_marcar_costeiros.py
Marca os municipios costeiros em municipios_brasil.is_costeiro e no GeoJSON.

Fonte autoritativa: IBGE GeoServer WFS
  camada CGMAT:qg_2021_280_municipioscosteiros
  - quadro = 280 (numero do Quadro Geografico 2021, nao eh a contagem)
  - a camada lista TODOS os 5572 municipios, cada um com flag cd_muncost:
      cd_muncost='1' -> costeiro/estuarino (Decreto 5300/2004, revisao IBGE 2021)
      cd_muncost='2' -> nao costeiro
  - 443 municipios costeiros no total.

Pre-requisito: ter rodado 08_baixar_malha_ibge.py antes.
"""
import json
import sqlite3
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
GEO = ROOT / "app" / "static" / "data" / "municipios_br.geojson"

WFS_URL = (
    "https://geoservicos.ibge.gov.br/geoserver/CGMAT/ows"
    "?service=WFS&version=2.0.0&request=GetFeature"
    "&typeNames=CGMAT:qg_2021_280_municipioscosteiros"
    "&outputFormat=application/json"
    "&propertyName=cd_mun,cd_muncost"
)


def p(msg: str) -> None:
    print(msg, flush=True)


def baixar_lista_ibge() -> set[str]:
    p("baixando lista oficial IBGE via WFS...")
    r = requests.get(WFS_URL, timeout=300)
    r.raise_for_status()
    p(f"  status {r.status_code}, {len(r.content)/1024:.0f} KB")
    fc = r.json()
    feats = fc.get("features", [])
    p(f"  {len(feats)} features (total nacional)")
    codes: set[str] = set()
    nao_costeiros = 0
    for f in feats:
        props = f.get("properties") or {}
        flag = str(props.get("cd_muncost", "")).strip()
        code = str(props.get("cd_mun", "")).strip()
        if len(code) != 7 or not code.isdigit():
            continue
        if flag == "1":
            codes.add(code)
        else:
            nao_costeiros += 1
    p(f"  costeiros (cd_muncost=1): {len(codes)}")
    p(f"  nao costeiros (cd_muncost=2): {nao_costeiros}")
    return codes


def main():
    if not GEO.exists():
        raise SystemExit(f"GeoJSON nao encontrado em {GEO}. Rode 08_baixar_malha_ibge.py antes.")

    ibge_codes = baixar_lista_ibge()
    if not ibge_codes:
        raise SystemExit("Falha: WFS nao retornou codigos costeiros.")

    p(f"lendo {GEO.name}...")
    gj = json.loads(GEO.read_text())
    feats = gj["features"]
    p(f"  {len(feats)} features")

    all_codes = {f["properties"]["code_muni"] for f in feats}
    missing = ibge_codes - all_codes
    if missing:
        p(f"  [WARN] {len(missing)} codigos IBGE nao encontrados na malha: {list(missing)[:5]}")

    p("atualizando DB...")
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("UPDATE municipios_brasil SET is_costeiro = 0")
    con.executemany(
        "UPDATE municipios_brasil SET is_costeiro = 1 WHERE code_muni = ?",
        [(c,) for c in ibge_codes],
    )
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM municipios_brasil WHERE is_costeiro=1").fetchone()[0]
    con.close()
    p(f"  is_costeiro=1 no DB: {n}")

    p("atualizando GeoJSON inline...")
    for f in feats:
        f["properties"]["is_costeiro"] = 1 if f["properties"]["code_muni"] in ibge_codes else 0
    GEO.write_text(json.dumps(gj, ensure_ascii=False))
    p(f"[OK] {GEO}")


if __name__ == "__main__":
    main()
