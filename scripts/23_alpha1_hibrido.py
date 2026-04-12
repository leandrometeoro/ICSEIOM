"""
23_alpha1_hibrido.py

Abordagem (D): baseline por mediana de estrato + LGBM no residuo.

Ideia
-----
Com n=201 autos, LGBM puro tem CV R^2 = 0.066 +/- 0.325 (instavel).
A mediana por estrato e um chao robusto e defensavel no Delphi.
O modelo entao so precisa aprender o DESVIO em relacao a esse chao.

Pipeline
--------
1) Estrato = (setor_pngc, tipo_infracao_top). Se o tipo_infracao nao estiver
   nos top-K do treino, cai em "outros".
2) Para cada estrato, calcula mediana_log(y) no treino.
   Se o estrato nao existir no treino (cold start no teste), usa mediana
   global do treino como fallback.
3) residuo_train = y_train - mediana_estrato_train
4) LGBM treina no residuo (mesmas features de s17.carregar()).
5) pred_final = mediana_estrato + lgbm_pred_residuo

Validacao
---------
KFold(5, shuffle=True, seed=42) sobre os 201 autos.
Reporta para cada fold:
  - R^2 baseline (so mediana_estrato)
  - R^2 LGBM puro (sem hibrido)
  - R^2 hibrido (mediana + residuo_lgbm)

Saida
-----
print das metricas por fold e media +/- desvio.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
from sklearn.metrics import r2_score
from sklearn.model_selection import KFold

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

# import dinamico do s17 (nome comeca com digito)
spec = importlib.util.spec_from_file_location(
    "s17", SCRIPTS / "17_treinar_modelos_alpha1.py"
)
s17 = importlib.util.module_from_spec(spec)
sys.modules["s17"] = s17
spec.loader.exec_module(s17)

import lightgbm as lgb


TOP_TIPOS_K = 6   # top-K tipos_infracao no treino para definir estrato
SEED = 42
N_SPLITS = 5


def estrato_key(setor: int, tipo: str, tipos_top: set[str]) -> tuple:
    t = tipo if tipo in tipos_top else "outros"
    return (int(setor), t)


def calcular_medianas(y_tr: np.ndarray,
                      setores_tr: list[int],
                      tipos_tr: list[str],
                      tipos_top: set[str]) -> tuple[dict, float]:
    buckets: dict[tuple, list[float]] = {}
    for yi, s, t in zip(y_tr, setores_tr, tipos_tr):
        k = estrato_key(s, t, tipos_top)
        buckets.setdefault(k, []).append(float(yi))
    medianas = {k: float(np.median(v)) for k, v in buckets.items()}
    mediana_global = float(np.median(y_tr))
    return medianas, mediana_global


def baseline_pred(setores: list[int],
                  tipos: list[str],
                  tipos_top: set[str],
                  medianas: dict,
                  mediana_global: float) -> np.ndarray:
    out = np.empty(len(setores), dtype=float)
    for i, (s, t) in enumerate(zip(setores, tipos)):
        k = estrato_key(s, t, tipos_top)
        out[i] = medianas.get(k, mediana_global)
    return out


def treinar_lgbm(X_tr: np.ndarray, y_tr: np.ndarray) -> lgb.Booster:
    dtrain = lgb.Dataset(X_tr, label=y_tr)
    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_data_in_leaf": 8,
        "feature_fraction": 0.85,
        "bagging_fraction": 0.85,
        "bagging_freq": 3,
        "verbose": -1,
        "seed": SEED,
    }
    return lgb.train(params, dtrain, num_boost_round=300)


def main() -> None:
    print("carregando features...", flush=True)
    X, y, feat_names, anos, setores, tipos = s17.carregar()
    n = len(y)
    print(f"n = {n} autos, {X.shape[1]} features")

    # top-K tipos no dataset inteiro (usado pra definir dominio de estrato)
    from collections import Counter
    cnt = Counter(tipos)
    tipos_top_global = {t for t, _ in cnt.most_common(TOP_TIPOS_K)}
    print(f"top-{TOP_TIPOS_K} tipos: {sorted(tipos_top_global)}")
    print(f"n estratos max = {5 * (TOP_TIPOS_K + 1)}")

    kf = KFold(n_splits=N_SPLITS, shuffle=True, random_state=SEED)

    res_baseline, res_lgbm, res_hib = [], [], []

    for fold, (tr_idx, te_idx) in enumerate(kf.split(X), 1):
        X_tr, X_te = X[tr_idx], X[te_idx]
        y_tr, y_te = y[tr_idx], y[te_idx]
        s_tr = [setores[i] for i in tr_idx]
        s_te = [setores[i] for i in te_idx]
        t_tr = [tipos[i]   for i in tr_idx]
        t_te = [tipos[i]   for i in te_idx]

        # 1) baseline mediana
        medianas, med_global = calcular_medianas(y_tr, s_tr, t_tr, tipos_top_global)
        base_tr = baseline_pred(s_tr, t_tr, tipos_top_global, medianas, med_global)
        base_te = baseline_pred(s_te, t_te, tipos_top_global, medianas, med_global)

        r2_base = r2_score(y_te, base_te)

        # 2) LGBM puro (referencia)
        booster_puro = treinar_lgbm(X_tr, y_tr)
        pred_puro = booster_puro.predict(X_te)
        r2_puro = r2_score(y_te, pred_puro)

        # 3) hibrido: LGBM no residuo
        resid_tr = y_tr - base_tr
        booster_hib = treinar_lgbm(X_tr, resid_tr)
        resid_pred_te = booster_hib.predict(X_te)
        pred_hib = base_te + resid_pred_te
        r2_hib = r2_score(y_te, pred_hib)

        res_baseline.append(r2_base)
        res_lgbm.append(r2_puro)
        res_hib.append(r2_hib)

        print(f"fold {fold}: baseline R2={r2_base:+.3f}  "
              f"lgbm R2={r2_puro:+.3f}  "
              f"hibrido R2={r2_hib:+.3f}")

    def resumo(nome: str, xs: list[float]) -> None:
        arr = np.array(xs)
        print(f"  {nome:10s} R2 = {arr.mean():+.3f} +/- {arr.std():.3f}  "
              f"(min {arr.min():+.3f}, max {arr.max():+.3f})")

    print("\nresumo (log-espaco):")
    resumo("baseline",  res_baseline)
    resumo("lgbm puro", res_lgbm)
    resumo("hibrido",   res_hib)


if __name__ == "__main__":
    main()
