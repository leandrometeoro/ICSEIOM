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
Atribuicao curada por literatura: cada poligono eh classificado pelo
centroide (lon,lat) em uma das 4 regioes reconhecidas de recife brasileiro,
e o ha total da regiao eh direcionado para o municipio-ancora oficial:

 - Banco de Abrolhos (PARNA Abrolhos, BA)      -> Caravelas/BA (2906907)
 - Parrachos de Maragogi/Porto de Pedras (AL)  -> Maragogi/AL (2704500)
 - Parcel Manuel Luis (APA Manuel Luis, MA)    -> Cururupu/MA (2103703)
 - Trindade/Martim Vaz (distrito de Vitoria)   -> Vitoria/ES (3205309)

Atribuicao por vizinho-mais-proximo via centroide do municipio falha aqui
porque centroides de municipios costeiros ficam no interior do poligono
municipal, enquanto os recifes estao offshore — o haversine acaba puxando
munis do interior (Itacare, Murici, Lauro de Freitas) que nao tem recife.
Com apenas 11 poligonos, curadoria manual eh defensavel e precisa.

Uso
---
    python3 scripts/33_ingest_unep_reefs_alpha5.py              # ano 2024 default
    python3 scripts/33_ingest_unep_reefs_alpha5.py --ano 2024
"""
from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
GEOJSON = ROOT / "data" / "mapbiomas" / "unep_reefs_brasil.geojson"
DOI_URL = "https://doi.org/10.34892/t2wk-5t34"

# Regioes curadas: nome, bbox (lon_min, lat_min, lon_max, lat_max), muni-ancora
REGIOES = [
    # Banco de Abrolhos (BA): PARNA Abrolhos, municipio-ancora Caravelas
    ("Abrolhos",        (-38.5, -18.5, -37.0, -13.0), "2906907"),
    # Costa dos Corais (APA PE+AL): Tamandare/Rio Formoso ate Maceio,
    # municipio-ancora Maragogi (portal turistico central da APA)
    ("Costa dos Corais",(-36.8, -10.6, -35.0,  -8.5), "2704500"),
    # Parcel Manuel Luis (APA): MA, ancora Cururupu
    ("Parcel Manuel L", (-45.2,  -1.3, -44.3,  -0.5), "2103703"),
    # Trindade e Martim Vaz: ilhas oceanicas, distrito de Vitoria/ES
    ("Trindade/M.Vaz",  (-29.5, -20.6, -28.7, -20.4), "3205309"),
]


def _classificar(lon: float, lat: float) -> tuple[str, str] | None:
    for nome, (xmin, ymin, xmax, ymax), code in REGIOES:
        if xmin <= lon <= xmax and ymin <= lat <= ymax:
            return nome, code
    return None


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

    # Atribui cada poligono a sua regiao curada
    ha_recife_por_muni: dict[str, float] = {}
    atribuicoes: list[tuple] = []
    orfaos: list[tuple] = []
    for f in data["features"]:
        p = f["properties"]
        km2 = float(p.get("gis_area_k") or 0)
        if km2 <= 0:
            continue
        cx, cy = _centroid(f["geometry"])
        cls = _classificar(cx, cy)
        if cls is None:
            orfaos.append((km2, cx, cy, p.get("loc_def", "")[:40]))
            continue
        regiao, code = cls
        ha = km2 * 100.0  # 1 km² = 100 ha
        ha_recife_por_muni[code] = ha_recife_por_muni.get(code, 0.0) + ha
        atribuicoes.append((regiao, code, km2, p.get("loc_def", "")[:30]))

    # Nome do municipio para log
    nome_por_code = {
        code: cur.execute(
            "SELECT nome||'/'||uf FROM municipios_brasil WHERE code_muni=?", (code,)
        ).fetchone()[0]
        for code in ha_recife_por_muni
    }
    print(f"[INFO] Atribuicoes curadas ({len(atribuicoes)} poligonos -> {len(ha_recife_por_muni)} municipios):")
    for regiao, code, km2, loc in atribuicoes:
        print(f"   {km2:8.2f} km² -> {nome_por_code[code]:25} [{regiao}]  [{loc}]")
    if orfaos:
        print(f"[WARN] {len(orfaos)} poligonos sem regiao (fora de bbox curada):")
        for km2, cx, cy, loc in orfaos:
            print(f"   {km2:8.2f} km² ({cx:.3f},{cy:.3f})  [{loc}]")
    total_km2 = sum(km2 for _, _, km2, _ in atribuicoes)
    print(f"[INFO] Total atribuido: {total_km2:.2f} km² = {total_km2*100:.0f} ha")

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
            "Atribuicao por vizinho mais proximo usando centroides IBGE dos "
            "443 municipios costeiros (populados via script 34). Cada poligono "
            "de recife eh atribuido ao municipio cujo centroide eh mais proximo "
            "do centroide do poligono. Trindade (ES) fica administrativamente "
            "em Vitoria mesmo distante ~1100 km da sede.",
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
