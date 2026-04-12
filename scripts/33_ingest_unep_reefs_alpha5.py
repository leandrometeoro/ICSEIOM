"""
33_ingest_unep_reefs_alpha5.py  —  ha de recife de coral por municipio
                                     via UNEP-WCMC Global Distribution of
                                     Coral Reefs (v4.1, 2021)

Complementa o script 32 (MapBiomas) preenchendo a coluna ha_recife em
alpha5_ecossistemas, que fica zerada no 32 porque MapBiomas cobre apenas
land cover.

Fonte
-----
    UNEP-WCMC & WorldFish Centre, WRI, TNC (2021). Global distribution of
    coral reefs. Version 4.1. Cambridge (UK): UNEP-WCMC.
    DOI: 10.34892/t2wk-5t34
    REST: https://data-gis.unep-wcmc.org/server/rest/services/
          HabitatsAndBiotopes/Global_Distribution_of_Coral_Reefs/FeatureServer/1

Arquivo local: data/mapbiomas/unep_reefs_brasil.geojson
(11 poligonos cobrindo a costa brasileira, total 697.57 km² = 69.757 ha)

Metodo
------
Para cada poligono:
 1. calcula centroide (media simples dos vertices).
 2. encontra o municipio costeiro mais proximo via haversine entre centroide
    do poligono e lat_centro/lon_centro dos 61 de municipios_costeiros.
 3. soma a area (km²) ao ha_recife do municipio escolhido.

Limitacao: so considera os 61 com centroide; Abrolhos (Caravelas/BA), Parcel
Manuel Luis (Cururupu/MA) e Parrachos (Maragogi/AL) caem nos vizinhos mais
proximos (Porto Seguro, Sao Luis, Maceio respectivamente). Para atribuicao
municipal fiel, adicionar centroides IBGE a todos os 443 costeiros.

Uso
---
    python3 scripts/33_ingest_unep_reefs_alpha5.py              # ano 2024 default
    python3 scripts/33_ingest_unep_reefs_alpha5.py --ano 2024
"""
from __future__ import annotations
import argparse
import json
import math
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
GEOJSON = ROOT / "data" / "mapbiomas" / "unep_reefs_brasil.geojson"
DOI_URL = "https://doi.org/10.34892/t2wk-5t34"


def _centroid(geom: dict) -> tuple[float, float]:
    """Centroide simples (media dos vertices) em (lon, lat)."""
    if geom["type"] == "Polygon":
        polys = [geom["coordinates"]]
    elif geom["type"] == "MultiPolygon":
        polys = geom["coordinates"]
    else:
        raise ValueError(geom["type"])
    xs, ys = [], []
    for poly in polys:
        for ring in poly:
            for x, y in ring:
                xs.append(x); ys.append(y)
    return sum(xs) / len(xs), sum(ys) / len(ys)


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def _load_coefs(cur):
    rows = {k: v for k, v in cur.execute(
        "SELECT chave, valor FROM parametros WHERE chave LIKE 'teeb_%'"
    )}
    base_row = cur.execute(
        "SELECT valor_texto FROM parametros WHERE chave='alpha5_base'"
    ).fetchone()
    base = (base_row[0] if base_row and base_row[0] else "global")
    return (
        rows.get("teeb_manguezal_global", 49950),
        rows.get("teeb_recife_global",    25575),
        rows.get("teeb_restinga_global",   2455),
        rows.get("teeb_manguezal_brasil",  1075),
        rows.get("teeb_recife_brasil",    25575),
        rows.get("teeb_restinga_brasil",   2455),
        base,
    )


def ingest(ano: int = 2024) -> None:
    if not GEOJSON.exists():
        raise SystemExit(f"Nao encontrado: {GEOJSON}")

    data = json.loads(GEOJSON.read_text())
    print(f"[INFO] {len(data['features'])} poligonos UNEP-WCMC")

    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row; cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")

    centroides = cur.execute(
        "SELECT code_muni, nome, uf, lat_centro, lon_centro "
        "FROM municipios_costeiros WHERE lat_centro IS NOT NULL"
    ).fetchall()
    print(f"[INFO] {len(centroides)} municipios costeiros com centroide")

    # Atribui cada poligono ao municipio mais proximo
    ha_recife_por_muni: dict[str, float] = {}
    atribuicoes: list[tuple] = []
    for f in data["features"]:
        p = f["properties"]
        km2 = float(p.get("gis_area_k") or 0)
        if km2 <= 0:
            continue
        cx, cy = _centroid(f["geometry"])  # lon, lat
        best = None
        best_d = float("inf")
        for m in centroides:
            d = _haversine_km(cx, cy, m["lon_centro"], m["lat_centro"])
            if d < best_d:
                best_d = d
                best = m
        code = best["code_muni"]
        ha = km2 * 100.0  # 1 km² = 100 ha
        ha_recife_por_muni[code] = ha_recife_por_muni.get(code, 0.0) + ha
        atribuicoes.append((p.get("loc_def", "?")[:30], km2, best["nome"], best["uf"], best_d))

    print(f"[INFO] Atribuicoes ({len(atribuicoes)} poligonos -> {len(ha_recife_por_muni)} municipios):")
    for loc, km2, nome, uf, d in atribuicoes:
        print(f"   {km2:8.2f} km² -> {nome}/{uf}  ({d:.0f} km)  [{loc}]")
    total_km2 = sum(km2 for _, km2, *_ in atribuicoes)
    print(f"[INFO] Total: {total_km2:.2f} km² = {total_km2*100:.0f} ha")

    # Atualiza alpha5 SOMENTE para os municipios atribuidos, recomputando os 3 valores
    c_m_g, c_r_g, c_t_g, c_m_b, c_r_b, c_t_b, base = _load_coefs(cur)
    atualizados = 0
    for code, ha_r in ha_recife_por_muni.items():
        row = cur.execute(
            "SELECT ha_manguezal, ha_restinga FROM alpha5_ecossistemas "
            "WHERE code_muni=? AND ano=?", (str(code).zfill(7), ano)
        ).fetchone()
        if row is None:
            print(f"[WARN] sem linha alpha5 para {code} ano={ano}, pulando")
            continue
        ha_m = row["ha_manguezal"] or 0.0
        ha_rt = row["ha_restinga"] or 0.0
        valor_global = ha_m * c_m_g + ha_r * c_r_g + ha_rt * c_t_g
        valor_brasil = ha_m * c_m_b + ha_r * c_r_b + ha_rt * c_t_b
        valor_ativo = valor_global if base == "global" else valor_brasil
        cur.execute(
            "UPDATE alpha5_ecossistemas SET ha_recife=?, "
            "valor_teeb_global_rs=?, valor_teeb_brasil_rs=?, valor_teeb_rs=?, "
            "fonte=? WHERE code_muni=? AND ano=?",
            (round(ha_r, 1),
             round(valor_global, 2),
             round(valor_brasil, 2),
             round(valor_ativo, 2),
             f"MapBiomas Col 9 + UNEP-WCMC Coral Reefs v4.1 + TEEB ({base})",
             str(code).zfill(7), ano),
        )
        atualizados += 1

    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao "
        "(fonte, ultima_safra, atualizado_em, url, observacoes, "
        " nome_humano, orgao, url_portal, descricao_uso, script, observacoes_metodologicas) "
        "VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "alpha5_recife_unep",
            "2021_v4.1",
            DOI_URL,
            f"UNEP-WCMC Coral Reefs v4.1: {len(atribuicoes)} poligonos, "
            f"{total_km2:.0f} km² = {total_km2*100:.0f} ha, atribuidos por "
            f"vizinho mais proximo aos {len(ha_recife_por_muni)} municipios.",
            "UNEP-WCMC Global Distribution of Coral Reefs v4.1",
            "UNEP-WCMC / WorldFish / WRI / TNC",
            "https://data.unep-wcmc.org/datasets/1",
            "hectares de recife de coral por municipio costeiro "
            "(Abrolhos, Parrachos de Alagoas, Parcel Manuel Luis, Trindade), "
            "valorados via coeficiente TEEB recife global/brasil.",
            "scripts/33_ingest_unep_reefs_alpha5.py",
            "Atribuicao por vizinho mais proximo dentro dos 61 municipios "
            "com centroide; Abrolhos cai em Porto Seguro/BA (deveria ser "
            "Caravelas), Parrachos cai em Maceio (deveria ser Maragogi). "
            "Para atribuicao fiel, adicionar centroide IBGE aos 443 "
            "costeiros e refazer a atribuicao.",
        ),
    )
    con.commit()
    con.close()

    print(f"[OK] alpha5 {ano}: {atualizados} municipios com ha_recife atualizado")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, default=2024)
    args = p.parse_args()
    ingest(ano=args.ano)
