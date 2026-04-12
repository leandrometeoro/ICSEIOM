"""
07_carregar_parametros.py   —   parametros globais, beta e chi

Carrega:
  - k (share LGAF no merito coletivo) — placeholder 0,30, pendente Delphi
  - coeficientes TEEB dupla base (global Costanza 2014 vs brasil CCARBON/USP)
  - alpha5_base: escolha da base ativa (texto, em parametros.valor_texto)
  - beta e chi anuais da LGAF

Sobre k
-------
ICSEIOM = k * (alpha1+...+alpha5) + beta - chi. O k representa a *fracao do
lucro social evitado atribuivel a LGAF*. Os 1-k restantes vao para os demais
atores da cadeia de resposta (IBAMA fiscalizacao, ICMBio protecao de UC,
Marinha contencao, Defesa Civil evacuacao, municipios alerta a populacao).
Valor 0,30 e placeholder ate a validacao Delphi descrita em
ICSEIOM_proposta.docx (painel de 3 internos IEAPM/CHM/DHN + 3 externos
UFRJ/INPE/IBAMA/ANP).

Sobre alpha5 dupla base
-----------------------
alpha5 pode ser calculado com dois conjuntos de coeficientes R$/ha/ano:

  global  (Costanza et al. 2014, The value of ecosystem services):
    manguezal  R$ 49.950   recife R$ 25.575   restinga R$ 2.455

  brasil  (CCARBON/USP para manguezal amazonico; Costanza para os demais
           ate haver valoracao brasileira especifica):
    manguezal  R$  1.075   recife R$ 25.575   restinga R$ 2.455

A diferenca de ~46x no manguezal reflete: Costanza usa media global
ponderada por servicos (carbono + pesqueiro + costa), CCARBON usa dados
in situ de sequestro e produtividade em manguezais amazonicos brasileiros.

Operador escolhe a base via /admin/fontes ou:
    UPDATE parametros SET valor_texto='brasil' WHERE chave='alpha5_base';
    python scripts/06_ingest_alpha5_ecossistemas.py
"""
import sqlite3, argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

# (chave, valor_real, valor_texto, unidade, descricao)
PARAMS = [
    ("k", 0.30, None, "",
     "Share LGAF no merito coletivo do lucro social evitado. "
     "ICSEIOM = k*(alpha1+...+alpha5)+beta-chi. Placeholder 0,30, "
     "pendente calibracao Delphi (painel IEAPM/CHM/DHN + externos)."),

    # --- TEEB global (Costanza 2014) ---
    ("teeb_manguezal_global", 49950, None, "R$/ha",
     "Costanza et al. 2014, media global USD 9.990/ha/ano x 5,0"),
    ("teeb_recife_global",    25575, None, "R$/ha",
     "Costanza et al. 2014, media global USD 5.115/ha/ano x 5,0"),
    ("teeb_restinga_global",   2455, None, "R$/ha",
     "Costanza et al. 2014, media global USD 491/ha/ano x 5,0"),

    # --- TEEB brasil ---
    ("teeb_manguezal_brasil",  1075, None, "R$/ha",
     "CCARBON/USP (2023), manguezal amazonico USD 215/ha/ano x 5,0"),
    ("teeb_recife_brasil",    25575, None, "R$/ha",
     "Sem valoracao brasileira especifica publicada; mantem Costanza 2014"),
    ("teeb_restinga_brasil",   2455, None, "R$/ha",
     "Sem valoracao brasileira especifica publicada; mantem Costanza 2014"),

    # --- base ativa ---
    ("alpha5_base", 0, "global", "",
     "Base ativa de valoracao TEEB do alpha5: 'global' (Costanza 2014) "
     "ou 'brasil' (CCARBON/USP para manguezal)"),

    # --- legado (apontam pra global; mantidos pra back-compat) ---
    ("teeb_manguezal", 49950, None, "R$/ha",
     "Legado: equivale a teeb_manguezal_global"),
    ("teeb_recife",    25575, None, "R$/ha",
     "Legado: equivale a teeb_recife_global"),
    ("teeb_restinga",   2455, None, "R$/ha",
     "Legado: equivale a teeb_restinga_global"),

    ("limiar_ideal",     0.90, None, "", "Limiar IDEAL do indicador"),
    ("limiar_aceitavel", 0.60, None, "", "Limiar ACEITAVEL do indicador"),
]

BETA_2024 = 1_250_000   # R$ receitas diretas (demo)
CHI_2024  = 8_600_000   # R$ custos anuais LGAF (demo)


def main(ano=2024):
    con = sqlite3.connect(DB); cur = con.cursor()
    # Garante que a coluna valor_texto existe (migracao aplicada em app/db.py,
    # mas o script pode ser rodado direto sem passar pelo app).
    cols = {r[1] for r in cur.execute("PRAGMA table_info(parametros)")}
    if "valor_texto" not in cols:
        cur.execute("ALTER TABLE parametros ADD COLUMN valor_texto TEXT")

    for chave, valor, valor_texto, unidade, desc in PARAMS:
        cur.execute(
            "INSERT OR REPLACE INTO parametros "
            "(chave, valor, valor_texto, unidade, descricao) VALUES (?,?,?,?,?)",
            (chave, valor, valor_texto, unidade, desc),
        )
    cur.execute(
        "INSERT OR REPLACE INTO beta_receitas_lgaf (ano, valor_rs, fonte) VALUES (?,?,?)",
        (ano, BETA_2024, "Tesouro Gerencial (demo)"),
    )
    cur.execute(
        "INSERT OR REPLACE INTO chi_custos_lgaf (ano, valor_rs, fonte) VALUES (?,?,?)",
        (ano, CHI_2024, "SIAFI (demo)"),
    )
    con.commit()
    con.close()
    print(f"[OK] Parametros, beta e chi para {ano} carregados.")
    print(f"     k=0,30 (share LGAF, placeholder Delphi)")
    print(f"     alpha5_base='global' (Costanza 2014) — operador pode trocar")

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--ano", type=int, default=2024)
    main(p.parse_args().ano)
