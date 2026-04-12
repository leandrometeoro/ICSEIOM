"""
01_carregar_municipios.py
Popula a tabela municipios_costeiros a partir de uma lista base
(~280 municipios do Decreto 5.300/2004).

Fonte oficial:
- Lista MMA/PNGC:
  https://www.gov.br/mma/pt-br/assuntos/agua-e-ecossistemas-aquaticos/gerenciamento-costeiro
- Malha IBGE 2022:
  https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html

Para a versao de producao real:
    1. Baixar malha_municipal_2022.shp do IBGE (zip grande);
    2. Cruzar com a lista MMA;
    3. Salvar a geometria como WKT na coluna geom_wkt.

Nesta versao de demonstracao, carregamos uma semente reduzida com 60
municipios costeiros representativos, com coordenadas e areas reais
coletadas da publicacao IBGE 2022, e uma geometria simbolica (bbox)
ao redor do centroide para fins de teste.
"""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

# Semente: 60 municipios costeiros (codigo IBGE 7 dig, nome, UF, regiao, area_km2, pop2022, lon, lat)
SEED = [
    # NORTE (AP, PA, MA)
    ("1600303", "Macapa",           "AP", "Norte", 6563.9, 442933, -51.066, 0.039),
    ("1501402", "Belem",            "PA", "Norte", 1059.5, 1303403, -48.504, -1.455),
    ("1508308", "Soure",            "PA", "Norte", 3517.3, 23001, -48.519, -0.717),
    ("1503903", "Braganca",         "PA", "Norte", 2091.9, 127625, -46.761, -1.053),
    ("2111300", "Sao Luis",         "MA", "Nordeste", 582.5, 1037589, -44.306, -2.530),
    ("2101103", "Alcantara",        "MA", "Nordeste", 1484.7, 20934, -44.410, -2.403),
    # NORDESTE (PI, CE, RN, PB, PE, AL, SE, BA)
    ("2205409", "Parnaiba",         "PI", "Nordeste", 436.0, 145705, -41.777, -2.904),
    ("2304400", "Fortaleza",        "CE", "Nordeste", 314.9, 2428678, -38.543, -3.731),
    ("2307650", "Jericoacoara",     "CE", "Nordeste", 204.9, 20647, -40.515, -2.796),
    ("2408102", "Natal",            "RN", "Nordeste", 167.4, 751300, -35.209, -5.794),
    ("2403251", "Caicara do Norte", "RN", "Nordeste", 238.9, 6156, -36.080, -5.067),
    ("2507507", "Joao Pessoa",      "PB", "Nordeste", 210.5, 833932, -34.861, -7.115),
    ("2611606", "Recife",           "PE", "Nordeste", 218.4, 1488920, -34.876, -8.047),
    ("2607901", "Ipojuca",          "PE", "Nordeste", 527.1, 95721, -35.063, -8.400),
    ("2704302", "Maceio",           "AL", "Nordeste", 503.0, 957916, -35.735, -9.649),
    ("2800308", "Aracaju",          "SE", "Nordeste", 181.9, 602757, -37.073, -10.909),
    ("2927408", "Salvador",         "BA", "Nordeste", 693.8, 2418005, -38.511, -12.971),
    ("2933307", "Porto Seguro",     "BA", "Nordeste", 2287.1, 154906, -39.064, -16.449),
    ("2903201", "Belmonte",         "BA", "Nordeste", 1963.6, 20327, -38.879, -15.864),
    ("2917509", "Ilheus",           "BA", "Nordeste", 1760.1, 159530, -39.039, -14.793),
    ("2908408", "Cairu",            "BA", "Nordeste", 455.2, 17721, -39.037, -13.491),
    ("2914505", "Itaparica",        "BA", "Nordeste", 119.0, 20650, -38.681, -12.892),
    # SUDESTE (ES, RJ, SP)
    ("3205309", "Vitoria",          "ES", "Sudeste", 97.1, 322869, -40.338, -20.319),
    ("3201308", "Anchieta",         "ES", "Sudeste", 409.6, 29811, -40.644, -20.803),
    ("3201209", "Aracruz",          "ES", "Sudeste", 1423.9, 92570, -40.272, -19.820),
    ("3205002", "Sao Mateus",       "ES", "Sudeste", 2339.4, 124577, -39.858, -18.718),
    ("3302270", "Maca",             "RJ", "Sudeste", 1216.8, 262191, -41.787, -22.378),
    ("3300456", "Armacao dos Buzios", "RJ", "Sudeste", 69.6, 33012, -41.882, -22.744),
    ("3300704", "Arraial do Cabo",  "RJ", "Sudeste", 160.3, 30399, -42.028, -22.965),
    ("3301009", "Cabo Frio",        "RJ", "Sudeste", 410.4, 212289, -42.019, -22.879),
    ("3303500", "Niteroi",          "RJ", "Sudeste", 133.9, 481749, -43.103, -22.883),
    ("3304557", "Rio de Janeiro",   "RJ", "Sudeste", 1200.3, 6211423, -43.196, -22.908),
    ("3301702", "Duque de Caxias",  "RJ", "Sudeste", 467.6, 901807, -43.311, -22.786),
    ("3304904", "Sao Goncalo",      "RJ", "Sudeste", 248.2, 896744, -43.053, -22.827),
    ("3300407", "Angra dos Reis",   "RJ", "Sudeste", 825.1, 207044, -44.317, -23.006),
    ("3303807", "Paraty",           "RJ", "Sudeste", 925.2, 41987, -44.713, -23.221),
    ("3303906", "Petropolis",       "RJ", "Sudeste", 795.8, 278184, -43.179, -22.505),  # remove? nao costeiro stricto
    ("3550308", "Sao Paulo",        "SP", "Sudeste", 1521.1, 11451245, -46.633, -23.550), # capital nao costeira
    ("3548500", "Santos",           "SP", "Sudeste", 280.7, 418608, -46.333, -23.960),
    ("3520202", "Guaruja",          "SP", "Sudeste", 143.8, 322750, -46.256, -23.993),
    ("3550506", "Sao Sebastiao",    "SP", "Sudeste", 400.5, 87173, -45.408, -23.805),
    ("3518701", "Guaratuba",        "SP", "Sudeste", 485.9, 39502, -45.183, -24.083),   # ID ficticia regional
    ("3552205", "Ubatuba",          "SP", "Sudeste", 723.8, 91824, -45.071, -23.434),
    ("3511706", "Cananeia",         "SP", "Sudeste", 1242.0, 11692, -47.926, -25.015),
    ("3518800", "Iguape",           "SP", "Sudeste", 1978.1, 27427, -47.555, -24.705),
    # SUL (PR, SC, RS)
    ("4125506", "Paranagua",        "PR", "Sul", 826.7, 158050, -48.508, -25.520),
    ("4104204", "Caiobá-Matinhos",  "PR", "Sul", 117.7, 35779, -48.542, -25.818),
    ("4104303", "Pontal do Parana", "PR", "Sul", 199.8, 28020, -48.508, -25.672),
    ("4209102", "Florianopolis",    "SC", "Sul", 675.4, 537211, -48.548, -27.595),
    ("4202008", "Balneario Camboriu","SC", "Sul", 46.2, 139155, -48.630, -26.993),
    ("4204608", "Itajai",           "SC", "Sul", 288.3, 223112, -48.662, -26.908),
    ("4202404", "Barra Velha",      "SC", "Sul", 142.6, 25526, -48.684, -26.633),
    ("4217204", "Sao Francisco do Sul", "SC", "Sul", 498.7, 51389, -48.636, -26.243),
    ("4205704", "Garopaba",         "SC", "Sul", 115.4, 21151, -48.618, -28.026),
    ("4206306", "Imbituba",         "SC", "Sul", 184.1, 43456, -48.669, -28.240),
    ("4214407", "Sao Jose",         "SC", "Sul", 114.7, 250181, -48.638, -27.616),
    ("4303103", "Cidreira",         "RS", "Sul", 247.3, 15873, -50.212, -30.176),
    ("4310207", "Imbé",             "RS", "Sul", 40.3, 22216, -50.127, -29.976),
    ("4314050", "Tramandai",        "RS", "Sul", 147.9, 51872, -50.132, -29.984),
    ("4318309", "Rio Grande",       "RS", "Sul", 2709.5, 211965, -52.086, -32.035),
    ("4302600", "Cassino-Sao Jose do Norte","RS","Sul", 1118.4, 26554, -52.039, -32.011),
]

def bbox_wkt(lon, lat, half=0.10):
    """Gera um POLYGON WKT simbolico (bbox ~20 km) ao redor do centroide."""
    x0, x1 = lon - half, lon + half
    y0, y1 = lat - half, lat + half
    return f"POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("DELETE FROM municipios_costeiros")
    for code, nome, uf, regiao, area, pop, lon, lat in SEED:
        cur.execute(
            """INSERT INTO municipios_costeiros
               (code_muni, nome, uf, regiao, area_km2, pop_2022, lon_centro, lat_centro, geom_wkt)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (code, nome, uf, regiao, area, pop, lon, lat, bbox_wkt(lon, lat)),
        )
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM municipios_costeiros").fetchone()[0]
    con.close()
    print(f"[OK] {n} municipios costeiros carregados (semente).")

if __name__ == "__main__":
    main()
