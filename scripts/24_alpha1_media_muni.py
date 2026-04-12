"""
24_alpha1_media_muni.py

Abordagem (E): media historica do proprio municipio, com fallback
hierarquico por similaridade.

Hierarquia de predicao
----------------------
  1) Municipio tem >=1 auto no treino  -> media log(valor) do municipio
  2) Fallback: mesmo setor_pngc + mesmo perfil de infra
     (ref, term, duto, eep) assinado como tupla
  3) Fallback: mesmo setor_pngc
  4) Fallback: media global do treino

Validacao: KFold(5, seed=42) sobre os 201 autos.
Compara com LGBM puro (referencia do script 23).
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import numpy as np
from sklearn.metrics import r2_score
from sklearn.model_selection import KFold

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

spec = importlib.util.spec_from_file_location(
    "s17", ROOT / "scripts" / "17_treinar_modelos_alpha1.py"
)
s17 = importlib.util.module_from_spec(spec)
sys.modules["s17"] = s17
spec.loader.exec_module(s17)

SEED = 42
N_SPLITS = 5


def carregar_meta() -> tuple[list[str], list[int], list[tuple]]:
    """Retorna, para cada auto usado por s17.carregar(), os metadados
    (code_muni, setor_pngc, infra_tuple) na MESMA ORDEM."""
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT f.seq_auto, f.code_muni, f.setor_pngc,
               f.tem_refinaria, f.tem_terminal, f.tem_duto, f.tem_campo_eep
        FROM mb_alpha1_auto_feat f
        JOIN mb_alpha1_autos a ON a.seq_auto = f.seq_auto
        WHERE f.log_pop IS NOT NULL AND f.log_pib_pc IS NOT NULL
          AND f.setor_pngc IS NOT NULL AND f.cluster_id IS NOT NULL
          AND a.relevante_oleo = 1
    """).fetchall()
    con.close()
    codes = [r[1] for r in rows]
    setores = [int(r[2]) for r in rows]
    infras = [(int(r[3]), int(r[4]), int(r[5]), int(r[6])) for r in rows]
    return codes, setores, infras


def treinar_tabelas(y_tr: np.ndarray,
                    codes_tr: list[str],
                    setores_tr: list[int],
                    infras_tr: list[tuple]) -> dict:
    por_muni: dict[str, list[float]] = {}
    por_setor_infra: dict[tuple, list[float]] = {}
    por_setor: dict[int, list[float]] = {}
    for yi, c, s, inf in zip(y_tr, codes_tr, setores_tr, infras_tr):
        por_muni.setdefault(c, []).append(float(yi))
        por_setor_infra.setdefault((s, inf), []).append(float(yi))
        por_setor.setdefault(s, []).append(float(yi))
    tabelas = {
        "muni":        {k: float(np.mean(v)) for k, v in por_muni.items()},
        "setor_infra": {k: float(np.mean(v)) for k, v in por_setor_infra.items()},
        "setor":       {k: float(np.mean(v)) for k, v in por_setor.items()},
        "global":      float(np.mean(y_tr)),
    }
    return tabelas


def prever(codes: list[str],
           setores: list[int],
           infras: list[tuple],
           tab: dict) -> tuple[np.ndarray, list[str]]:
    out = np.empty(len(codes), dtype=float)
    via = []
    for i, (c, s, inf) in enumerate(zip(codes, setores, infras)):
        if c in tab["muni"]:
            out[i] = tab["muni"][c]; via.append("muni")
        elif (s, inf) in tab["setor_infra"]:
            out[i] = tab["setor_infra"][(s, inf)]; via.append("setor_infra")
        elif s in tab["setor"]:
            out[i] = tab["setor"][s]; via.append("setor")
        else:
            out[i] = tab["global"]; via.append("global")
    return out, via


def main() -> None:
    print("carregando features e metadados...", flush=True)
    X, y, feat_names, anos, setores_s17, tipos = s17.carregar()
    codes, setores, infras = carregar_meta()
    assert len(codes) == len(y), f"{len(codes)} != {len(y)}"
    n = len(y)
    print(f"n = {n} autos")
    n_munis = len(set(codes))
    print(f"n municipios distintos = {n_munis}")

    kf = KFold(n_splits=N_SPLITS, shuffle=True, random_state=SEED)

    res = []
    vias_all = []

    for fold, (tr_idx, te_idx) in enumerate(kf.split(y), 1):
        y_tr, y_te = y[tr_idx], y[te_idx]
        c_tr = [codes[i]   for i in tr_idx]
        c_te = [codes[i]   for i in te_idx]
        s_tr = [setores[i] for i in tr_idx]
        s_te = [setores[i] for i in te_idx]
        i_tr = [infras[i]  for i in tr_idx]
        i_te = [infras[i]  for i in te_idx]

        tab = treinar_tabelas(y_tr, c_tr, s_tr, i_tr)
        pred_te, via_te = prever(c_te, s_te, i_te, tab)
        r2 = r2_score(y_te, pred_te)
        res.append(r2)
        vias_all.extend(via_te)

        from collections import Counter
        vc = Counter(via_te)
        print(f"fold {fold}: R2={r2:+.3f}  vias={dict(vc)}")

    arr = np.array(res)
    print(f"\nmedia muni (hierarquico) R2 = {arr.mean():+.3f} +/- {arr.std():.3f}"
          f"  (min {arr.min():+.3f}, max {arr.max():+.3f})")

    from collections import Counter
    print(f"distribuicao de vias (agregado): {dict(Counter(vias_all))}")


if __name__ == "__main__":
    main()
