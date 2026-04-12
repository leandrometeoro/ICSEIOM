"""
25_alpha1_estimador_final.py

Calcula o estimador final de alpha1 por municipio costeiro (443 costeiros
do Decreto 5300/2004 ja cadastrados em mb_features_muni).

Definicao
---------
alpha1_hat(muni) = E[valor_auto | muni]
  estimado como media geometrica dos autos historicos IBAMA-oleo
  (filtro estrito, script 21).

Hierarquia de fallback (para municipios sem historico):
  1) media log do proprio municipio        (via = "muni")
  2) media log do mesmo (setor_pngc, perfil_infra)  (via = "setor_infra")
  3) media log do mesmo setor_pngc                  (via = "setor")
  4) media log global                               (via = "global")

O perfil de infra e uma tupla (ref, term, duto, eep) em mb_infra_oleo.

Saida
-----
Tabela mb_alpha1_estimativa (criada/substituida):
  code_muni   TEXT PRIMARY KEY
  alpha1_hat  REAL   -- valor esperado por auto, em R$ corrigidos
  via         TEXT   -- 'muni' | 'setor_infra' | 'setor' | 'global'
  n_base      INT    -- n de autos usados na estimativa (muni) ou no grupo
  log_media   REAL   -- media de log(valor_real) usada (para auditoria)

Alem disso imprime um resumo por via e atualiza mb_alpha1_multa com o
novo valor agregado? NAO — essa tabela guarda a soma historica observada,
que e outro conceito. O estimador vive numa tabela separada.
"""
from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"


def p(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # 1) autos relevantes com valor positivo
    autos = cur.execute("""
        SELECT a.code_muni, f.log_valor_real
        FROM mb_alpha1_autos a
        JOIN mb_alpha1_auto_feat f USING (seq_auto)
        WHERE a.relevante_oleo = 1
          AND f.log_valor_real IS NOT NULL
    """).fetchall()
    p(f"autos relevantes com log_valor_real: {len(autos)}")

    # 2) dimensoes municipais
    dims = cur.execute("""
        SELECT m.code_muni,
               m.setor_pngc,
               COALESCE(i.tem_refinaria, 0),
               COALESCE(i.tem_terminal,  0),
               COALESCE(i.tem_duto,      0),
               COALESCE(i.tem_campo_eep, 0)
        FROM mb_features_muni m
        LEFT JOIN mb_infra_oleo i ON i.code_muni = m.code_muni
    """).fetchall()
    p(f"municipios costeiros cadastrados: {len(dims)}")

    dim_muni: dict[str, tuple[int, tuple[int, int, int, int]]] = {}
    for code, setor, ref, term, duto, eep in dims:
        dim_muni[code] = (int(setor) if setor is not None else 0,
                          (int(ref), int(term), int(duto), int(eep)))

    # 3) agrupa log_valor por muni / por (setor,infra) / por setor / global
    por_muni:        dict[str, list[float]] = defaultdict(list)
    por_setor_infra: dict[tuple, list[float]] = defaultdict(list)
    por_setor:       dict[int,   list[float]] = defaultdict(list)
    todos: list[float] = []

    sem_dim = 0
    for code, logv in autos:
        todos.append(float(logv))
        por_muni[code].append(float(logv))
        if code in dim_muni:
            setor, infra = dim_muni[code]
            por_setor_infra[(setor, infra)].append(float(logv))
            por_setor[setor].append(float(logv))
        else:
            sem_dim += 1
    if sem_dim:
        p(f"AVISO: {sem_dim} autos com code_muni fora de mb_features_muni")

    def media(xs: list[float]) -> float:
        return sum(xs) / len(xs)

    media_global = media(todos)
    p(f"media log global = {media_global:.4f}  "
      f"(R$ {math.exp(media_global):,.0f})")

    # 4) compoe estimativa para cada um dos 443 costeiros
    cur.execute("DROP TABLE IF EXISTS mb_alpha1_estimativa")
    cur.execute("""
        CREATE TABLE mb_alpha1_estimativa (
            code_muni  TEXT PRIMARY KEY,
            alpha1_hat REAL NOT NULL,
            via        TEXT NOT NULL,
            n_base     INTEGER NOT NULL,
            log_media  REAL NOT NULL
        )
    """)

    vias_cnt: dict[str, int] = defaultdict(int)
    linhas: list[tuple] = []

    for code, (setor, infra) in dim_muni.items():
        if por_muni.get(code):
            xs = por_muni[code]
            via = "muni"
        elif por_setor_infra.get((setor, infra)):
            xs = por_setor_infra[(setor, infra)]
            via = "setor_infra"
        elif por_setor.get(setor):
            xs = por_setor[setor]
            via = "setor"
        else:
            xs = todos
            via = "global"

        log_m = media(xs)
        alpha1_hat = math.exp(log_m)
        linhas.append((code, alpha1_hat, via, len(xs), log_m))
        vias_cnt[via] += 1

    cur.executemany(
        "INSERT INTO mb_alpha1_estimativa "
        "(code_muni, alpha1_hat, via, n_base, log_media) VALUES (?,?,?,?,?)",
        linhas,
    )
    con.commit()

    p("\ndistribuicao por via de fallback:")
    for v in ("muni", "setor_infra", "setor", "global"):
        p(f"  {v:12s}: {vias_cnt[v]:4d} municipios")

    # amostras
    p("\namostras (top 5 alpha1_hat):")
    for code, val, via, n, _ in sorted(linhas, key=lambda r: -r[1])[:5]:
        nome = cur.execute(
            "SELECT nome || ' - ' || uf FROM municipios_costeiros WHERE code_muni=?",
            (code,),
        ).fetchone()
        p(f"  {code} {nome[0] if nome else '?'}: "
          f"R$ {val:,.0f} (via={via}, n={n})")

    p("\nok — mb_alpha1_estimativa criada")
    con.close()


if __name__ == "__main__":
    main()
