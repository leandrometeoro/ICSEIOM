"""
06_ingest_alpha5_ecossistemas.py   —   alpha5: servicos ecossistemicos

Metodologia
-----------
    alpha5(muni) = ha_manguezal * coef_manguezal
                 + ha_recife    * coef_recife
                 + ha_restinga  * coef_restinga

A coluna valor_teeb_rs armazena o valor da base ATIVA, conforme
parametros.alpha5_base ('global'|'brasil'). As colunas
valor_teeb_global_rs e valor_teeb_brasil_rs guardam ambos os calculos
para permitir alternar sem re-ingerir, via admin/fontes ou SQL:

    UPDATE parametros SET valor_texto='brasil' WHERE chave='alpha5_base';
    -- e depois
    from app.calc import set_alpha5_base
    set_alpha5_base('brasil')

Valoracao (R$/ha/ano)
---------------------
    base 'global' (Costanza 2014, media global):
        manguezal  R$ 49.950
        recife     R$ 25.575
        restinga   R$  2.455

    base 'brasil' (CCARBON/USP + Costanza fallback):
        manguezal  R$  1.075   <- USD 215/ha/ano x 5,0 (Amazonia)
        recife     R$ 25.575   <- sem valoracao BR, mantem Costanza
        restinga   R$  2.455   <- sem valoracao BR, mantem Costanza

Areas (ha) por municipio
------------------------
PRELIMINAR: usa estimativas regionais do Atlas dos Manguezais do Brasil
(ICMBio 2018) escaladas pela area municipal. TODO: substituir por dados
reais do MapBiomas Colecao 10 Costeiro e Marinho, que fornece hectares
por municipio de manguezal, restinga arborea e recifes em serie anual
1985-2024, com licenca CC BY 4.0.

    https://plataforma.brasil.mapbiomas.org/
    https://brasil.mapbiomas.org/colecoes-mapbiomas-cm/

Uso
---
    python scripts/06_ingest_alpha5_ecossistemas.py --ano 2024
"""
import sqlite3, argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
URL = "https://brasil.mapbiomas.org/colecoes-mapbiomas-cm/"

# Estimativa preliminar de areas (ha) por regiao — ICMBio Atlas dos Manguezais
# (2018) + SOS Mata Atlantica; ordem de grandeza para municipio tipico.
# TODO: substituir por MapBiomas Col 10 (CSV por code_muni).
AREA_MANG   = {"Norte": 4800, "Nordeste": 1600, "Sudeste": 350, "Sul": 180}
AREA_RECIFE = {"Norte":    0, "Nordeste":  220, "Sudeste":  60, "Sul":   0}
AREA_REST   = {"Norte":  250, "Nordeste":  420, "Sudeste": 380, "Sul": 280}


def _load_coefs(cur) -> dict:
    rows = cur.execute(
        "SELECT chave, valor FROM parametros WHERE chave LIKE 'teeb_%'"
    ).fetchall()
    return {k: v for k, v in rows}


def _load_base_ativa(cur) -> str:
    r = cur.execute(
        "SELECT valor_texto FROM parametros WHERE chave='alpha5_base'"
    ).fetchone()
    return (r[0] if r and r[0] else "global")


def _ensure_columns(cur) -> None:
    cols = {r[1] for r in cur.execute("PRAGMA table_info(alpha5_ecossistemas)")}
    if "valor_teeb_global_rs" not in cols:
        cur.execute("ALTER TABLE alpha5_ecossistemas ADD COLUMN valor_teeb_global_rs REAL")
    if "valor_teeb_brasil_rs" not in cols:
        cur.execute("ALTER TABLE alpha5_ecossistemas ADD COLUMN valor_teeb_brasil_rs REAL")
    cols_p = {r[1] for r in cur.execute("PRAGMA table_info(parametros)")}
    if "valor_texto" not in cols_p:
        cur.execute("ALTER TABLE parametros ADD COLUMN valor_texto TEXT")


def ingest_real(ano: int = 2024):
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row; cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")
    _ensure_columns(cur)

    coefs = _load_coefs(cur)
    c_m_g = coefs.get("teeb_manguezal_global", 49950)
    c_r_g = coefs.get("teeb_recife_global",    25575)
    c_t_g = coefs.get("teeb_restinga_global",   2455)
    c_m_b = coefs.get("teeb_manguezal_brasil",  1075)
    c_r_b = coefs.get("teeb_recife_brasil",    25575)
    c_t_b = coefs.get("teeb_restinga_brasil",   2455)
    base = _load_base_ativa(cur)

    muns = cur.execute(
        "SELECT code_muni, regiao, area_km2 FROM municipios_costeiros"
    ).fetchall()
    cur.execute("DELETE FROM alpha5_ecossistemas WHERE ano=?", (ano,))

    total_global = 0.0
    total_brasil = 0.0
    for r in muns:
        code, regiao, area_km2 = r["code_muni"], r["regiao"], r["area_km2"]
        escala = (area_km2 or 200) / 500
        ha_m  = AREA_MANG.get(regiao, 300) * escala
        ha_r  = AREA_RECIFE.get(regiao, 50) * escala
        ha_rt = AREA_REST.get(regiao, 200) * escala

        valor_global = ha_m * c_m_g + ha_r * c_r_g + ha_rt * c_t_g
        valor_brasil = ha_m * c_m_b + ha_r * c_r_b + ha_rt * c_t_b
        valor_ativo  = valor_global if base == "global" else valor_brasil

        cur.execute(
            "INSERT INTO alpha5_ecossistemas "
            "(code_muni, ano, ha_manguezal, ha_recife, ha_restinga, "
            " valor_teeb_rs, valor_teeb_global_rs, valor_teeb_brasil_rs, fonte) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (code, ano, round(ha_m, 1), round(ha_r, 1), round(ha_rt, 1),
             round(valor_ativo, 2), round(valor_global, 2), round(valor_brasil, 2),
             f"preliminar: ICMBio Atlas + TEEB ({base})"),
        )
        total_global += valor_global
        total_brasil += valor_brasil

    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao "
        "(fonte, ultima_safra, atualizado_em, url, observacoes) "
        "VALUES (?, ?, datetime('now'), ?, ?)",
        ("alpha5_ecossistemas", str(ano), URL,
         f"Areas preliminares (ICMBio Atlas regional). Valoracao dupla: "
         f"global Costanza 2014, brasil CCARBON/USP. Base ativa: {base}. "
         f"TODO: substituir areas por MapBiomas Col 10 Costeiro."),
    )
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM alpha5_ecossistemas WHERE ano=?", (ano,)).fetchone()[0]
    con.close()
    print(f"[OK] alpha5 {ano}: {n} municipios")
    print(f"     total base global : R$ {total_global/1e9:.2f} B")
    print(f"     total base brasil : R$ {total_brasil/1e9:.2f} B")
    print(f"     base ativa        : {base}")


# Back-compat: nome antigo chamado pelo pipeline legacy.
def ingest_demo(ano: int = 2024):
    ingest_real(ano)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, default=2024)
    ingest_real(p.parse_args().ano)
