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

# Semente: 61 municipios costeiros (codigo IBGE 7 dig, nome, UF, regiao, area_km2, pop2022, lon, lat)
# Codigos IBGE conferidos contra municipios_brasil (IBGE Malhas Territoriais 2022) em 2026-04-12.
SEED = [
    # NORTE (AP, PA, MA)
    ("1600303", "Macapa",                    "AP", "Norte",    6563.9, 442933, -51.066, 0.039),
    ("1501402", "Belem",                     "PA", "Norte",    1059.5, 1303403, -48.504, -1.455),
    ("1501709", "Bragança",                  "PA", "Norte",    2091.9, 127625, -46.761, -1.053),
    ("1507904", "Soure",                     "PA", "Norte",    3517.3, 23001, -48.519, -0.717),
    ("2100204", "Alcântara",                 "MA", "Nordeste", 1484.7, 20934, -44.41, -2.403),
    ("2111300", "Sao Luis",                  "MA", "Nordeste", 582.5, 1037589, -44.306, -2.53),
    # NORDESTE (PI, CE, RN, PB, PE, AL, SE, BA)
    ("2207702", "Parnaíba",                  "PI", "Nordeste", 436.0, 145705, -41.777, -2.904),
    ("2304400", "Fortaleza",                 "CE", "Nordeste", 314.9, 2428678, -38.543, -3.731),
    ("2307254", "Jijoca de Jericoacoara",    "CE", "Nordeste", 204.9, 20647, -40.515, -2.796),
    ("2401859", "Caiçara do Norte",          "RN", "Nordeste", 238.9, 6156, -36.08, -5.067),
    ("2408102", "Natal",                     "RN", "Nordeste", 167.4, 751300, -35.209, -5.794),
    ("2507507", "Joao Pessoa",               "PB", "Nordeste", 210.5, 833932, -34.861, -7.115),
    ("2607208", "Ipojuca",                   "PE", "Nordeste", 527.1, 95721, -35.063, -8.4),
    ("2611606", "Recife",                    "PE", "Nordeste", 218.4, 1488920, -34.876, -8.047),
    ("2704302", "Maceio",                    "AL", "Nordeste", 503.0, 957916, -35.735, -9.649),
    ("2800308", "Aracaju",                   "SE", "Nordeste", 181.9, 602757, -37.073, -10.909),
    ("2903409", "Belmonte",                  "BA", "Nordeste", 1963.6, 20327, -38.879, -15.864),
    ("2905404", "Cairu",                     "BA", "Nordeste", 455.2, 17721, -39.037, -13.491),
    ("2913606", "Ilhéus",                    "BA", "Nordeste", 1760.1, 159530, -39.039, -14.793),
    ("2916104", "Itaparica",                 "BA", "Nordeste", 119.0, 20650, -38.681, -12.892),
    ("2925303", "Porto Seguro",              "BA", "Nordeste", 2287.1, 154906, -39.064, -16.449),
    ("2927408", "Salvador",                  "BA", "Nordeste", 693.8, 2418005, -38.511, -12.971),
    # SUDESTE (ES, RJ, SP)
    ("3200409", "Anchieta",                  "ES", "Sudeste", 409.6, 29811, -40.644, -20.803),
    ("3200607", "Aracruz",                   "ES", "Sudeste", 1423.9, 92570, -40.272, -19.82),
    ("3204906", "São Mateus",                "ES", "Sudeste", 2339.4, 124577, -39.858, -18.718),
    ("3205309", "Vitoria",                   "ES", "Sudeste", 97.1, 322869, -40.338, -20.319),
    ("3300100", "Angra dos Reis",            "RJ", "Sudeste", 825.1, 207044, -44.317, -23.006),
    ("3300233", "Armação dos Búzios",        "RJ", "Sudeste", 69.6, 33012, -41.882, -22.744),
    ("3300258", "Arraial do Cabo",           "RJ", "Sudeste", 160.3, 30399, -42.028, -22.965),
    ("3300704", "Cabo Frio",                 "RJ", "Sudeste", 410.4, 212289, -42.019, -22.879),
    ("3301702", "Duque de Caxias",           "RJ", "Sudeste", 467.6, 901807, -43.311, -22.786),
    ("3302403", "Macaé",                     "RJ", "Sudeste", 1216.8, 262191, -41.787, -22.378),
    ("3303302", "Niterói",                   "RJ", "Sudeste", 133.9, 481749, -43.103, -22.883),
    ("3303807", "Paraty",                    "RJ", "Sudeste", 925.2, 41987, -44.713, -23.221),
    ("3303906", "Petropolis",                "RJ", "Sudeste", 795.8, 278184, -43.179, -22.505),  # nao costeiro stricto
    ("3304557", "Rio de Janeiro",            "RJ", "Sudeste", 1200.3, 6211423, -43.196, -22.908),
    ("3304904", "Sao Goncalo",               "RJ", "Sudeste", 248.2, 896744, -43.053, -22.827),
    ("3509908", "Cananéia",                  "SP", "Sudeste", 1242.0, 11692, -47.926, -25.015),
    ("3518701", "Guarujá",                   "SP", "Sudeste", 143.8, 322750, -46.256, -23.993),
    ("3520301", "Iguape",                    "SP", "Sudeste", 1978.1, 27427, -47.555, -24.705),
    ("3548500", "Santos",                    "SP", "Sudeste", 280.7, 418608, -46.333, -23.96),
    ("3550308", "Sao Paulo",                 "SP", "Sudeste", 1521.1, 11451245, -46.633, -23.55),  # capital nao costeira
    ("3550704", "São Sebastião",             "SP", "Sudeste", 400.5, 87173, -45.408, -23.805),
    ("3555406", "Ubatuba",                   "SP", "Sudeste", 723.8, 91824, -45.071, -23.434),
    # SUL (PR, SC, RS)
    ("4109609", "Guaratuba",                 "PR", "Sul", 485.9, 39502, -48.5758, -25.8808),
    ("4115705", "Matinhos",                  "PR", "Sul", 117.7, 35779, -48.542, -25.818),
    ("4118204", "Paranaguá",                 "PR", "Sul", 826.7, 158050, -48.508, -25.52),
    ("4119954", "Pontal do Paraná",          "PR", "Sul", 199.8, 28020, -48.508, -25.672),
    ("4202008", "Balneario Camboriu",        "SC", "Sul", 46.2, 139155, -48.63, -26.993),
    ("4202107", "Barra Velha",               "SC", "Sul", 142.6, 25526, -48.684, -26.633),
    ("4205407", "Florianópolis",             "SC", "Sul", 675.4, 537211, -48.548, -27.595),
    ("4205704", "Garopaba",                  "SC", "Sul", 115.4, 21151, -48.618, -28.026),
    ("4207304", "Imbituba",                  "SC", "Sul", 184.1, 43456, -48.669, -28.24),
    ("4208203", "Itajaí",                    "SC", "Sul", 288.3, 223112, -48.662, -26.908),
    ("4216206", "São Francisco do Sul",      "SC", "Sul", 498.7, 51389, -48.636, -26.243),
    ("4216602", "São José",                  "SC", "Sul", 114.7, 250181, -48.638, -27.616),
    ("4305454", "Cidreira",                  "RS", "Sul", 247.3, 15873, -50.212, -30.176),
    ("4310330", "Imbé",                      "RS", "Sul", 40.3, 22216, -50.127, -29.976),
    ("4315602", "Rio Grande",                "RS", "Sul", 2709.5, 211965, -52.086, -32.035),
    ("4318507", "São José do Norte",         "RS", "Sul", 1118.4, 26554, -52.039, -32.011),
    ("4321600", "Tramandaí",                 "RS", "Sul", 147.9, 51872, -50.132, -29.984),
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
