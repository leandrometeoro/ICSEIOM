"""
34_carregar_centroides_ibge.py — popula lat_centro/lon_centro em
municipios_brasil (5570 linhas) a partir de app/static/data/municipios_br.geojson.

Centroide calculado como media simples dos vertices do poligono/multipoligono
— suficiente para atribuicao por vizinho mais proximo (nao precisa ser o
centroide geometrico real). Rodar uma vez apos criar/expandir o banco.

Uso
---
    python3 scripts/34_carregar_centroides_ibge.py
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
GEOJSON = ROOT / "app" / "static" / "data" / "municipios_br.geojson"


def _centroid(geom: dict) -> tuple[float, float]:
    if geom["type"] == "Polygon":
        polys = [geom["coordinates"]]
    elif geom["type"] == "MultiPolygon":
        polys = geom["coordinates"]
    else:
        raise ValueError(geom["type"])
    xs, ys = [], []
    for poly in polys:
        for x, y in poly[0]:  # anel externo so
            xs.append(x); ys.append(y)
    return sum(xs) / len(xs), sum(ys) / len(ys)


def main() -> None:
    if not GEOJSON.exists():
        raise SystemExit(f"Nao encontrado: {GEOJSON}")
    data = json.loads(GEOJSON.read_text())
    print(f"[INFO] {len(data['features'])} features no geojson")

    con = sqlite3.connect(DB); cur = con.cursor()
    # garante colunas (caso banco tenha sido criado antes da migration)
    existing = {r[1] for r in cur.execute("PRAGMA table_info(municipios_brasil)")}
    if "lat_centro" not in existing:
        cur.execute("ALTER TABLE municipios_brasil ADD COLUMN lat_centro REAL")
    if "lon_centro" not in existing:
        cur.execute("ALTER TABLE municipios_brasil ADD COLUMN lon_centro REAL")

    atualizados = 0
    nao_encontrados = 0
    for f in data["features"]:
        code = str(f["properties"]["code_muni"]).zfill(7)
        lon, lat = _centroid(f["geometry"])
        r = cur.execute(
            "UPDATE municipios_brasil SET lat_centro=?, lon_centro=? WHERE code_muni=?",
            (lat, lon, code),
        )
        if r.rowcount == 0:
            nao_encontrados += 1
        else:
            atualizados += r.rowcount

    con.commit()
    n_total = cur.execute("SELECT count(*) FROM municipios_brasil WHERE lat_centro IS NOT NULL").fetchone()[0]
    n_cost = cur.execute("SELECT count(*) FROM municipios_brasil WHERE is_costeiro=1 AND lat_centro IS NOT NULL").fetchone()[0]
    con.close()

    print(f"[OK] {atualizados} linhas atualizadas, {nao_encontrados} geojson sem match no banco")
    print(f"     {n_total}/5570 munis com centroide")
    print(f"     {n_cost}/443 costeiros com centroide")


if __name__ == "__main__":
    main()
