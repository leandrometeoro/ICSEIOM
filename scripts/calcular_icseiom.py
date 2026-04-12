"""
calcular_icseiom.py   —   API de calculo do ICSEIOM

Dado um evento (data, lat, lon, raio_km, foi_poluente), identifica os
municipios costeiros impactados, recupera os valores de cada alpha na
safra anual correspondente, e retorna o ICSEIOM em R$.

Nao depende de nenhuma consulta externa. Todos os dados sao lidos do
banco SQLite pre-carregado em db/icseiom.db.
"""
from __future__ import annotations
import sqlite3
import math
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

def _km(lon1, lat1, lon2, lat2):
    """Distancia Haversine em km."""
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

@dataclass
class ResultadoICSEIOM:
    id_evento: Optional[int]
    municipios: list
    ano_safra: int
    alpha1_rs: float
    alpha2_rs: float
    alpha3_rs: float
    alpha4_rs: float
    alpha5_rs: float
    beta_rs: float
    chi_rs: float
    k: float
    icseiom_rs: float

def calcular_icseiom(
    data_evento: str,
    lon: float,
    lat: float,
    raio_km: float,
    foi_poluente: bool = True,
    db_path: Path = DB,
) -> ResultadoICSEIOM:
    con = sqlite3.connect(db_path); cur = con.cursor()

    ano = int(data_evento[:4])
    # usa safra mais recente disponivel ate o ano do evento
    row = cur.execute(
        "SELECT MAX(ano) FROM alpha1_multa_ambiental WHERE ano <= ?", (ano,)
    ).fetchone()
    ano_safra = row[0] or ano

    # 1) municipios afetados (brute force com Haversine sobre o centroide)
    muns = cur.execute(
        "SELECT code_muni, nome, uf, lon_centro, lat_centro FROM municipios_costeiros"
    ).fetchall()
    afetados = []
    for code, nome, uf, lon_c, lat_c in muns:
        d = _km(lon, lat, lon_c, lat_c)
        if d <= raio_km:
            # fracao linear de 1 no centro a 0 na borda
            frac = max(0.0, 1 - d / raio_km) if raio_km > 0 else 1.0
            afetados.append((code, nome, uf, d, frac))

    if not afetados:
        con.close()
        return ResultadoICSEIOM(None, [], ano_safra, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    # 2) somatorio ponderado de cada alpha
    def soma(tabela, col):
        total = 0.0
        for code, _, _, _, frac in afetados:
            v = cur.execute(
                f"SELECT {col} FROM {tabela} WHERE code_muni=? AND ano=?",
                (code, ano_safra),
            ).fetchone()
            total += (v[0] if v else 0) * frac
        return total

    a1 = soma("alpha1_multa_ambiental", "valor_rs")
    a2 = soma("alpha2_pesca",          "valor_rs")
    a3 = soma("alpha3_turismo",        "vab_aloj_rs")
    a4 = soma("alpha4_saude",          "custo_rs")
    a5 = soma("alpha5_ecossistemas",   "valor_teeb_rs")

    # 3) se nao foi poluente, α₁ (multa evitada) peso total; se foi,
    #    α₁ nao conta como evitada (foi aplicada) — regra do Grupo 4
    if foi_poluente:
        a1 = 0.0

    beta = cur.execute(
        "SELECT valor_rs FROM beta_receitas_lgaf WHERE ano=?", (ano_safra,)
    ).fetchone()
    beta = beta[0] if beta else 0.0

    chi = cur.execute(
        "SELECT valor_rs FROM chi_custos_lgaf WHERE ano=?", (ano_safra,)
    ).fetchone()
    chi = chi[0] if chi else 0.0

    k = cur.execute("SELECT valor FROM parametros WHERE chave='k'").fetchone()
    k = k[0] if k else 0.30

    # Para um unico evento, beta e chi sao rateados pelo numero esperado
    # de eventos anuais (ordem 10) — ajustavel via parametro.
    rateio = 10
    beta_ev = beta / rateio
    chi_ev  = chi  / rateio

    icseiom = k * (a1 + a2 + a3 + a4 + a5) + beta_ev - chi_ev

    con.close()
    return ResultadoICSEIOM(
        id_evento=None,
        municipios=[{"code": c, "nome": n, "uf": u, "dist_km": round(d, 2), "frac": round(f, 3)}
                    for c, n, u, d, f in afetados],
        ano_safra=ano_safra,
        alpha1_rs=round(a1, 2), alpha2_rs=round(a2, 2), alpha3_rs=round(a3, 2),
        alpha4_rs=round(a4, 2), alpha5_rs=round(a5, 2),
        beta_rs=round(beta_ev, 2), chi_rs=round(chi_ev, 2),
        k=k, icseiom_rs=round(icseiom, 2),
    )

def imprimir(res: ResultadoICSEIOM):
    print("═" * 62)
    print(f"  ICSEIOM — Calculo do evento (safra {res.ano_safra})")
    print("═" * 62)
    print(f"  Municipios no raio: {len(res.municipios)}")
    for m in res.municipios[:10]:
        print(f"    • {m['nome']} ({m['uf']})  {m['dist_km']:>6} km  frac={m['frac']}")
    if len(res.municipios) > 10:
        print(f"    ... +{len(res.municipios)-10} outros")
    print(f"\n  α₁ multa ambiental evitada : R$ {res.alpha1_rs:>14,.2f}")
    print(f"  α₂ pesca                   : R$ {res.alpha2_rs:>14,.2f}")
    print(f"  α₃ turismo                 : R$ {res.alpha3_rs:>14,.2f}")
    print(f"  α₄ saude                   : R$ {res.alpha4_rs:>14,.2f}")
    print(f"  α₅ ecossistemas            : R$ {res.alpha5_rs:>14,.2f}")
    print(f"  Σα                         : R$ {sum([res.alpha1_rs,res.alpha2_rs,res.alpha3_rs,res.alpha4_rs,res.alpha5_rs]):>14,.2f}")
    print(f"  k aplicado                 : {res.k}")
    print(f"  β (rateio por evento)      : R$ {res.beta_rs:>14,.2f}")
    print(f"  χ (rateio por evento)      : R$ {res.chi_rs:>14,.2f}")
    print("─" * 62)
    print(f"  ICSEIOM do evento          : R$ {res.icseiom_rs:>14,.2f}")
    print("═" * 62)

if __name__ == "__main__":
    # autoteste: incidente ficticio ao largo de Cabo Frio
    r = calcular_icseiom("2026-04-05", lon=-42.02, lat=-22.95,
                         raio_km=25, foi_poluente=False)
    imprimir(r)
