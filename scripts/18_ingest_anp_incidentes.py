"""
18_ingest_anp_incidentes.py

Ingestao dos incidentes operacionais ANP (Res. 44/2009 / 882/2022) —
notificacoes feitas por concessionarias de E&P via SISO-Incidentes.

Fonte oficial:
  https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/incidentes-seguranca-operacional

Arquivos (CSV ISO-8859-1, delimitador ';', decimal ','):
  incidentes.csv               — uma linha por incidente
  incidentes-substancias.csv   — N linhas por incidente (substancia + volume m3)
  incidentes-classificacao.csv — classificacao de impacto
  incidentes-tipo.csv          — tipo operacional

Pipeline:
  1. Baixa os 4 CSVs (ou reusa cache em /tmp/anp).
  2. Parseia incidentes.csv extraindo (Numero, data, lat, lon, empresa, cnpj).
  3. Parseia substancias.csv e filtra APENAS substancias de oleo/hidrocarboneto.
  4. Reverse-geocode: cada incidente com lat/lon valida -> municipio costeiro
     mais proximo via haversine sobre os centroides de municipios_brasil
     (is_costeiro = 1). Incidentes sem lat/lon sao descartados.
  5. Agrega por (code_muni, ano) em mb_anp_incidentes:
       n_incidentes_oleo, vol_oleo_m3, vol_oleo_max_m3,
       n_empresas_distintas.

Esses agregados viram features no modelo de alpha1 (script 15b + 17)
para capturar "oportunidade de derrame" por municipio-ano.

Uso:
    python scripts/18_ingest_anp_incidentes.py --download   # baixa e cacheia
    python scripts/18_ingest_anp_incidentes.py              # reusa cache
"""
import argparse
import csv
import math
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
CACHE = Path("/tmp/anp")

URLS = {
    "incidentes": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/issm/incidentes.csv",
    "substancias": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/issm/incidentes-substancias.csv",
    "tipo": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/issm/incidentes-tipo.csv",
    "classificacao": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/issm/incidentes-classificacao.csv",
}

# Substancias consideradas "oleo" para alpha1. Criterio amplo: qualquer
# hidrocarboneto, fluido de perfuracao/lubrificante, agua oleosa.
KEYWORDS_OLEO = (
    "oleo", "petroleo", "hidrocarboneto", "diesel", "gasolina",
    "combustivel", "lubrificante", "fluido hidraulico",
    "fluido sintetico", "agua oleosa", "querosene", "nafta",
    "parafina", "oleoso",
)

# Excluir explicitamente: gas, agua nao-oleosa, produtos quimicos secos,
# MEG, soda, metanol etc.
KEYWORDS_EXCLUIR = (
    "gas natural", "gas sulf", "agua produzida", "agua de inje",
    "metanol", "etanol", "soda", "meg", "nitrogenio",
)

R_TERRA = 6371.0


def p(msg: str) -> None:
    print(msg, flush=True)


def slug(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s.lower()).strip()


def eh_oleo(substancia: str) -> bool:
    s = slug(substancia)
    if not s:
        return False
    for kw in KEYWORDS_EXCLUIR:
        if kw in s:
            return False
    for kw in KEYWORDS_OLEO:
        if kw in s:
            return True
    return False


def parse_vol(s: str) -> float | None:
    if s is None:
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 0 else None
    except ValueError:
        return None


def parse_data(s: str) -> tuple[int, int] | None:
    """Retorna (ano, mes) a partir de 'DD-MM-YYYY' ou 'DD/MM/YYYY'."""
    if not s:
        return None
    s = s.strip().replace("/", "-")
    m = re.match(r"(\d{2})-(\d{2})-(\d{4})", s)
    if m:
        dia, mes, ano = m.groups()
        a, me = int(ano), int(mes)
        if 1990 <= a <= 2030 and 1 <= me <= 12:
            return a, me
    return None


def parse_float(s: str) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R_TERRA * math.asin(math.sqrt(a))


def download_all():
    CACHE.mkdir(parents=True, exist_ok=True)
    for nome, url in URLS.items():
        target = CACHE / f"{nome}.csv"
        if target.exists() and target.stat().st_size > 1024:
            p(f"[cache] {nome}.csv ja baixado ({target.stat().st_size/1024:.0f} KB)")
            continue
        p(f"baixando {nome}.csv...")
        r = requests.get(url, timeout=600, stream=True)
        r.raise_for_status()
        target.write_bytes(r.content)
        p(f"  {len(r.content)/1024:.0f} KB")


def _poly_centroid(coords):
    """Centroide simples (media dos vertices) de um MultiPolygon/Polygon GeoJSON."""
    xs, ys = [], []
    def walk(node):
        if isinstance(node, (list, tuple)) and node and isinstance(node[0], (int, float)):
            xs.append(node[0]); ys.append(node[1])
        elif isinstance(node, (list, tuple)):
            for c in node:
                walk(c)
    walk(coords)
    if not xs:
        return None, None
    return sum(xs) / len(xs), sum(ys) / len(ys)


def carregar_costeiros(con) -> list[tuple[str, float, float]]:
    import json
    # lista de costeiros no DB
    codes = {r[0] for r in con.execute(
        "SELECT code_muni FROM municipios_brasil WHERE is_costeiro = 1"
    ).fetchall()}
    geo = ROOT / "app" / "static" / "data" / "municipios_br.geojson"
    with geo.open() as f:
        d = json.load(f)
    out = []
    for feat in d["features"]:
        code = feat["properties"].get("code_muni")
        if code not in codes:
            continue
        lon, lat = _poly_centroid(feat["geometry"]["coordinates"])
        if lon is not None:
            out.append((code, lon, lat))
    return out


def geocode_mais_proximo(lat, lon, costeiros, max_km=None):
    """Incidentes offshore em aguas federais ainda sao atribuidos ao muni
    costeiro mais proximo (mesma logica do IBAMA em autos offshore)."""
    best_code, best_d = None, 1e18
    for code, lo, la in costeiros:
        d = haversine(lat, lon, la, lo)
        if d < best_d:
            best_d = d
            best_code = code
    if max_km is not None and best_d > max_km:
        return None, best_d
    return best_code, best_d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true")
    args = ap.parse_args()

    if args.download:
        download_all()

    inc_path = CACHE / "incidentes.csv"
    sub_path = CACHE / "substancias.csv"
    if not inc_path.exists() or not sub_path.exists():
        p("ERRO: CSVs nao encontrados em /tmp/anp. Rode com --download.")
        sys.exit(1)

    con = sqlite3.connect(DB)
    costeiros = carregar_costeiros(con)
    p(f"municipios costeiros com centroide: {len(costeiros)}")

    # Fase 1: carregar substancias (N por incidente) e filtrar oleo
    p("fase 1/3: filtrando substancias de oleo...")
    vol_por_incidente: dict[str, float] = defaultdict(float)
    subs_por_incidente: dict[str, list[str]] = defaultdict(list)
    n_linhas_sub, n_com_vol, n_oleo = 0, 0, 0
    with sub_path.open(encoding="latin-1", newline="") as f:
        rdr = csv.DictReader(f, delimiter=";")
        for row in rdr:
            n_linhas_sub += 1
            num = (row.get("Numero") or "").strip()
            sub = (row.get("Substancias") or "").strip()
            vol = parse_vol(row.get("Volume") or "")
            if not num or not sub or vol is None:
                continue
            n_com_vol += 1
            if not eh_oleo(sub):
                continue
            n_oleo += 1
            vol_por_incidente[num] += vol
            subs_por_incidente[num].append(sub)
    p(f"  linhas substancia: {n_linhas_sub}, com volume: {n_com_vol}, de oleo: {n_oleo}")
    p(f"  incidentes unicos com oleo+volume: {len(vol_por_incidente)}")

    # Fase 2: parseia incidentes e geocode
    p("fase 2/3: geocoding dos incidentes...")
    agg: dict[tuple[str, int], dict] = {}
    n_tot, n_sem_coord, n_sem_data, n_fora, n_match = 0, 0, 0, 0, 0
    with inc_path.open(encoding="latin-1", newline="") as f:
        rdr = csv.DictReader(f, delimiter=";")
        for row in rdr:
            n_tot += 1
            num = (row.get("Numero") or "").strip()
            if num not in vol_por_incidente:
                continue  # soh queremos incidentes de oleo com volume

            lat = parse_float(row.get("Latitude") or "")
            lon = parse_float(row.get("Longitude") or "")
            if lat is None or lon is None or lat == 0 or lon == 0:
                n_sem_coord += 1
                continue

            d = parse_data(row.get("Data_estimada_do_incidente") or "") \
                or parse_data(row.get("Data_da_primeira_observacao") or "") \
                or parse_data(row.get("Data_de_criacao") or "")
            if d is None:
                n_sem_data += 1
                continue
            ano, _mes = d

            code, dist = geocode_mais_proximo(lat, lon, costeiros)
            n_match += 1

            vol = vol_por_incidente[num]
            empresa = (row.get("Empresa") or "").strip()
            k = (code, ano)
            if k not in agg:
                agg[k] = {"n": 0, "vol_sum": 0.0, "vol_max": 0.0, "emps": set()}
            agg[k]["n"] += 1
            agg[k]["vol_sum"] += vol
            agg[k]["vol_max"] = max(agg[k]["vol_max"], vol)
            if empresa:
                agg[k]["emps"].add(empresa)

    p(f"  incidentes lidos: {n_tot}")
    p(f"  sem coordenadas : {n_sem_coord}")
    p(f"  sem data        : {n_sem_data}")
    p(f"  fora costa 80km : {n_fora}")
    p(f"  geocoded        : {n_match}")
    p(f"  celulas (muni,ano) com incidentes: {len(agg)}")

    # Fase 3: persistir
    p("fase 3/3: gravando mb_anp_incidentes...")
    con.execute("DROP TABLE IF EXISTS mb_anp_incidentes")
    con.execute("""
        CREATE TABLE mb_anp_incidentes (
            code_muni      TEXT NOT NULL,
            ano            INTEGER NOT NULL,
            n_incidentes   INTEGER NOT NULL,
            vol_oleo_m3    REAL NOT NULL,
            vol_max_m3     REAL NOT NULL,
            n_empresas     INTEGER NOT NULL,
            PRIMARY KEY (code_muni, ano)
        )
    """)
    con.execute("CREATE INDEX idx_mb_anp_ano ON mb_anp_incidentes(ano)")

    rows_ins = [
        (k[0], k[1], v["n"], round(v["vol_sum"], 4),
         round(v["vol_max"], 4), len(v["emps"]))
        for k, v in agg.items()
    ]
    con.executemany(
        "INSERT INTO mb_anp_incidentes VALUES (?,?,?,?,?,?)", rows_ins
    )

    anos = sorted({k[1] for k in agg.keys()})
    safra = f"{min(anos)}-{max(anos)}" if anos else "—"
    vol_total = sum(v["vol_sum"] for v in agg.values())
    p(f"[OK] mb_anp_incidentes: {len(rows_ins)} celulas, volume total {vol_total:.1f} m3")
    p(f"     safra: {safra}")

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
            "mb_anp_incidentes",
            "Notificacoes de incidentes ANP (SISO)",
            "ANP",
            safra,
            datetime.utcnow().isoformat(timespec="seconds"),
            URLS["incidentes"],
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/incidentes-seguranca-operacional",
            "Volume derramado de oleo e n. de incidentes por (muni, ano). "
            "Vira feature no modelo de alpha1 para capturar oportunidade "
            "de derrame por municipio.",
            "scripts/18_ingest_anp_incidentes.py",
            f"{n_match} incidentes de oleo com lat/lon validos geocoded para "
            f"{len(set(k[0] for k in agg.keys()))} munis costeiros dentro de "
            f"80 km. Res. 44/2009 e 882/2022. Volume total: {vol_total:.1f} m3.",
        ),
    )
    con.commit()
    con.close()


if __name__ == "__main__":
    main()
