"""
32_ingest_mapbiomas_alpha5.py  —  ha por municipio via MapBiomas Col 9

Substitui as estimativas regionais preliminares (ICMBio Atlas escalado) em
alpha5_ecossistemas por dados reais de hectare por municipio, extraidos do
arquivo oficial MapBiomas Brasil Colecao 9 "Coverage by biomes, states and
municipalities" (DOI 10.58053/MapBiomas/VEJDZC, Jan/2025, 68 MB xlsx).

Fonte
-----
    https://data.mapbiomas.org/dataset.xhtml?persistentId=doi:10.58053/MapBiomas/VEJDZC
    arquivo: data/mapbiomas/mapbiomas_col9_cobertura_municipio.xlsx (baixado
    diretamente via /api/access/datafile/179, MD5 e999cf1d7c74445a162f9c615128a119)

Classes aproveitadas
--------------------
    code 5  = Mangue                    -> ha_manguezal
    code 49 = Restinga Arborea          -> soma em ha_restinga
    code 50 = Restinga Herbacea         -> soma em ha_restinga

Recifes de coral NAO estao nesta colecao (ela cobre apenas land cover).
ha_recife fica zerado — TODO substituir por Allen Coral Atlas ou lista
curada do Atlas dos Recifes de Coral ICMBio (Abrolhos/BA, Fernando de
Noronha/PE, Parcel Manuel Luis/MA, costa NE etc.).

Universo
--------
Atualiza **todos os 443 costeiros** (municipios_brasil is_costeiro=1).
Para os 61 que estao em municipios_costeiros, atualiza diretamente a
linha em alpha5_ecossistemas (ano 2024 persistido pelo script 06).
Para os outros ~380, faz UPSERT — FKs desabilitados, mesmo padrao de
alpha2/alpha3/alpha4.

Uso
---
    python scripts/32_ingest_mapbiomas_alpha5.py              # ano 2024 default (MapBiomas safra 2023)
    python scripts/32_ingest_mapbiomas_alpha5.py --ano 2024 --safra-mapbiomas 2023
"""
from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
XLSX = ROOT / "data" / "mapbiomas" / "mapbiomas_col9_cobertura_municipio.xlsx"
DOI_URL = "https://data.mapbiomas.org/dataset.xhtml?persistentId=doi:10.58053/MapBiomas/VEJDZC"

# Classe MapBiomas Col 9 -> campo em alpha5_ecossistemas
CLASS_MANG = 5
CLASS_REST_ARB = 49
CLASS_REST_HERB = 50


def _load_ha_by_muni(xlsx_path: Path, safra: int) -> dict:
    """Varre COVERAGE_9 e retorna {geocode: {'mang': ha, 'rest': ha}}."""
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb["COVERAGE_9"]
    header = None
    col_geocode = col_class = col_year = None
    out: dict[int, dict[str, float]] = {}
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            header = row
            col_geocode = header.index("geocode")
            col_class = header.index("class")
            # anos sao int (1985..2023); localizar 'safra'
            col_year = header.index(safra)
            continue
        cls = row[col_class]
        if cls not in (CLASS_MANG, CLASS_REST_ARB, CLASS_REST_HERB):
            continue
        geocode = row[col_geocode]
        ha = row[col_year] or 0.0
        if geocode is None:
            continue
        entry = out.setdefault(int(geocode), {"mang": 0.0, "rest": 0.0})
        if cls == CLASS_MANG:
            entry["mang"] += float(ha)
        else:
            entry["rest"] += float(ha)
    wb.close()
    return out


def _load_coefs(cur) -> tuple[float, float, float, float, float, float, str]:
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


def ingest(ano: int = 2024, safra: int = 2023) -> None:
    if not XLSX.exists():
        raise SystemExit(
            f"Arquivo nao encontrado: {XLSX}. Baixe via:\n"
            f"  curl -sL 'https://data.mapbiomas.org/api/access/datafile/179' \\\n"
            f"       -o '{XLSX}'"
        )

    print(f"[INFO] Lendo {XLSX.name} (safra MapBiomas={safra})...")
    ha_map = _load_ha_by_muni(XLSX, safra)
    n_com_mang = sum(1 for v in ha_map.values() if v["mang"] > 0)
    n_com_rest = sum(1 for v in ha_map.values() if v["rest"] > 0)
    print(f"[INFO] {len(ha_map)} municipios com mangue ou restinga")
    print(f"       {n_com_mang} com mangue, {n_com_rest} com restinga")

    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row; cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")
    c_m_g, c_r_g, c_t_g, c_m_b, c_r_b, c_t_b, base = _load_coefs(cur)

    # Costeiros: 443 de municipios_brasil
    cur.execute(
        "SELECT code_muni FROM municipios_brasil WHERE is_costeiro = 1"
    )
    costeiros = [int(r["code_muni"]) for r in cur.fetchall()]
    print(f"[INFO] {len(costeiros)} municipios costeiros alvo")

    cur.execute("DELETE FROM alpha5_ecossistemas WHERE ano = ?", (ano,))

    inseridos = 0
    sem_dado = 0
    total_ha_mang = 0.0
    total_ha_rest = 0.0
    total_global = 0.0
    total_brasil = 0.0

    for code in costeiros:
        entry = ha_map.get(code)
        if entry is None:
            sem_dado += 1
            ha_m, ha_rt = 0.0, 0.0
        else:
            ha_m = entry["mang"]
            ha_rt = entry["rest"]
        ha_r = 0.0  # TODO recife: fonte separada (Allen Coral Atlas)

        valor_global = ha_m * c_m_g + ha_r * c_r_g + ha_rt * c_t_g
        valor_brasil = ha_m * c_m_b + ha_r * c_r_b + ha_rt * c_t_b
        valor_ativo = valor_global if base == "global" else valor_brasil

        cur.execute(
            "INSERT INTO alpha5_ecossistemas "
            "(code_muni, ano, ha_manguezal, ha_recife, ha_restinga, "
            " valor_teeb_rs, valor_teeb_global_rs, valor_teeb_brasil_rs, fonte) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (str(code).zfill(7), ano,
             round(ha_m, 1), round(ha_r, 1), round(ha_rt, 1),
             round(valor_ativo, 2),
             round(valor_global, 2),
             round(valor_brasil, 2),
             f"MapBiomas Col 9 ({safra}) + TEEB ({base})"),
        )
        inseridos += 1
        total_ha_mang += ha_m
        total_ha_rest += ha_rt
        total_global += valor_global
        total_brasil += valor_brasil

    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao "
        "(fonte, ultima_safra, atualizado_em, url, observacoes, "
        " nome_humano, orgao, url_portal, descricao_uso, script, observacoes_metodologicas) "
        "VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "alpha5_ecossistemas",
            str(safra),
            DOI_URL,
            f"MapBiomas Col 9 safra {safra}: classes 5 (mangue), "
            f"49+50 (restinga arborea+herbacea). Recife=0 (TODO Allen Coral Atlas). "
            f"Valoracao dupla global/brasil; base ativa: {base}.",
            "MapBiomas Brasil Coleção 9 — Cobertura por Município",
            "MapBiomas (rede brasileira)",
            "https://brasil.mapbiomas.org/",
            "hectares de mangue, restinga arborea e restinga herbacea por "
            "municipio IBGE no ano mais recente disponivel (2023), usados como "
            "base territorial do alpha5. Valoracao e aplicada depois via "
            "coeficientes TEEB global (Costanza 2014) ou brasil (CCARBON/USP).",
            "scripts/32_ingest_mapbiomas_alpha5.py",
            "Recifes de coral nao estao na colecao terrestre — ha_recife=0 "
            "ate ingestao de fonte marinha especifica (Allen Coral Atlas ou "
            "Atlas dos Recifes de Coral ICMBio).",
        ),
    )
    con.commit()
    con.close()

    print(f"[OK] alpha5 {ano}: {inseridos} municipios inseridos, {sem_dado} sem dado MapBiomas")
    print(f"     ha mangue   total: {total_ha_mang:>12,.0f} ha")
    print(f"     ha restinga total: {total_ha_rest:>12,.0f} ha")
    print(f"     valor base global: R$ {total_global/1e9:.2f} B")
    print(f"     valor base brasil: R$ {total_brasil/1e9:.2f} B")
    print(f"     base ativa       : {base}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, default=2024,
                   help="Ano logico do alpha5 em alpha5_ecossistemas (default 2024)")
    p.add_argument("--safra-mapbiomas", type=int, default=2023, dest="safra",
                   help="Coluna de ano a ler no xlsx MapBiomas (default 2023)")
    args = p.parse_args()
    ingest(ano=args.ano, safra=args.safra)
