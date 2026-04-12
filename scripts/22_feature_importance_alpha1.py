"""Treina LGBM no dataset filtrado e imprime feature importance."""
from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
import numpy as np
import lightgbm as lgb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

spec = importlib.util.spec_from_file_location("s17", ROOT / "scripts" / "17_treinar_modelos_alpha1.py")
s17 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s17)

X, y, feat_names, anos, setores, tipos = s17.carregar()
print(f"n={len(y)} features={len(feat_names)}")

# estatisticas por feature
nnz = (X != 0).sum(axis=0)
variance = X.var(axis=0)
print("\nFeatures com nnz<10 (quase constantes):")
for i, (n, v, name) in enumerate(zip(nnz, variance, feat_names)):
    if n < 10:
        print(f"  {name:30s} nnz={int(n):4d}  var={v:.4f}")

params = {
    "objective": "regression", "metric": "mae",
    "learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 30,
    "feature_fraction": 0.7, "bagging_fraction": 0.8, "bagging_freq": 5,
    "lambda_l1": 0.5, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
}
lgb_tr = lgb.Dataset(X, label=y, feature_name=feat_names)
booster = lgb.train(params, lgb_tr, num_boost_round=300)

imp_split = booster.feature_importance(importance_type="split")
imp_gain = booster.feature_importance(importance_type="gain")

# ordenar por gain
order = np.argsort(-imp_gain)
print("\nFeature importance (ordenado por gain):")
print(f"{'feature':30s} {'split':>8s} {'gain':>14s}  nnz")
for i in order:
    print(f"  {feat_names[i]:30s} {imp_split[i]:8d} {imp_gain[i]:14.2f}  {int(nnz[i]):4d}")

# features com gain < 1 e nnz<20 = candidatas a drop
print("\nCandidatas a drop (gain<1 OU nnz<10):")
for i in order[::-1]:
    if imp_gain[i] < 1 or nnz[i] < 10:
        print(f"  {feat_names[i]}")
