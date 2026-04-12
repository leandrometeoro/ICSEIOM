"""
17b_walkforward_alpha1.py

Walk-forward validation do modelo alpha1 (LGBM + Ridge).

Simula operacao realista: re-treina o modelo a cada ano com todos os dados
ateh o ano anterior, e usa o ano alvo como teste out-of-sample.

Passos:
  treino ateh 2022 -> predicao 2023
  treino ateh 2023 -> predicao 2024
  treino ateh 2024 -> predicao 2025

Feature pipeline identico a 17_treinar_modelos_alpha1.py (importado como
modulo via importlib). Pesos de amostra iguais: importance weighting por
tipo_infracao + recency exponencial com meia-vida 4 anos.

Saidas:
  app/static/charts/alpha1_walkforward_<ano>.png   (scatter por ano alvo)
  app/static/charts/alpha1_walkforward_curva.png   (MAE e MdAPE por ano)
  mb_alpha1_walkforward (tabela) com metricas por (ano, modelo)
"""
import importlib.util
import math
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

import lightgbm as lgb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
CHARTS = ROOT / "app" / "static" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)

NAVY = "#21295C"
GOLD = "#C8A13A"
TEAL = "#1C7293"

ANOS_ALVO = [2023, 2024, 2025]


def p(msg: str) -> None:
    print(msg, flush=True)


def importar_script17():
    spec = importlib.util.spec_from_file_location(
        "s17", ROOT / "scripts" / "17_treinar_modelos_alpha1.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def mdape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    yt = np.exp(y_true)
    yp = np.exp(y_pred)
    err = np.abs(yp - yt) / np.clip(yt, 1.0, None)
    return float(np.median(err) * 100.0)


def r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - y_true.mean()) ** 2))
    if ss_tot <= 0:
        return float("nan")
    return 1.0 - ss_res / ss_tot


def pesos_treino(tipos_tr: list[str], tipos_te: list[str],
                 anos_tr: np.ndarray, ano_ancora: int) -> np.ndarray:
    cnt_tr = Counter(tipos_tr)
    cnt_te = Counter(tipos_te)
    all_tipos = set(cnt_tr) | set(cnt_te)
    n_tr = len(tipos_tr)
    n_te = len(tipos_te)
    K = max(len(all_tipos), 1)
    w_map = {}
    for t in all_tipos:
        p_tr = (cnt_tr.get(t, 0) + 1) / (n_tr + K)
        p_te = (cnt_te.get(t, 0) + 1) / (n_te + K)
        w_map[t] = float(np.clip(p_te / p_tr, 0.1, 10.0))
    iw = np.array([w_map[t] for t in tipos_tr], dtype=float)
    rec = np.power(2.0, (anos_tr.astype(float) - ano_ancora) / 4.0)
    w = iw * rec
    w = w / w.mean()
    return w


def treinar_e_prever(X_tr, y_tr, w_tr, X_te, feat_names):
    scaler = StandardScaler().fit(X_tr)
    Xs_tr = scaler.transform(X_tr)
    Xs_te = scaler.transform(X_te)

    ridge = Ridge(alpha=1.0).fit(Xs_tr, y_tr, sample_weight=w_tr)
    yh_ridge = ridge.predict(Xs_te)

    lgb_tr = lgb.Dataset(X_tr, label=y_tr, weight=w_tr, feature_name=feat_names)
    params = {
        "objective": "regression", "metric": "mae",
        "learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 60,
        "feature_fraction": 0.7, "bagging_fraction": 0.8, "bagging_freq": 5,
        "lambda_l1": 0.5, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
    }
    booster = lgb.train(params, lgb_tr, num_boost_round=300)
    yh_lgbm = booster.predict(X_te)
    return yh_ridge, yh_lgbm


def plot_scatter(y_true, y_pred, titulo, path, cor):
    fig, ax = plt.subplots(figsize=(5.2, 5.2))
    lo = float(min(y_true.min(), y_pred.min())) - 0.5
    hi = float(max(y_true.max(), y_pred.max())) + 0.5
    ax.plot([lo, hi], [lo, hi], ls="--", lw=1.0, color="#555")
    ax.scatter(y_true, y_pred, s=28, alpha=0.55, color=cor, edgecolor="none")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_xlabel("log(valor real) observado")
    ax.set_ylabel("log(valor real) previsto")
    ax.set_title(titulo)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def plot_curva(resultados, path):
    anos = [r["ano"] for r in resultados]
    mae_lgbm = [r["lgbm"]["mae"] for r in resultados]
    mae_ridge = [r["ridge"]["mae"] for r in resultados]
    mdape_lgbm = [r["lgbm"]["mdape"] for r in resultados]
    mdape_ridge = [r["ridge"]["mdape"] for r in resultados]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))

    ax1.plot(anos, mae_lgbm, "-o", color=GOLD, lw=2, label="LightGBM")
    ax1.plot(anos, mae_ridge, "-s", color=TEAL, lw=2, label="Ridge")
    ax1.set_xlabel("ano alvo")
    ax1.set_ylabel("MAE(log)")
    ax1.set_title("erro medio por ano (walk-forward)")
    ax1.set_xticks(anos)
    ax1.grid(True, alpha=0.25)
    ax1.legend()

    ax2.plot(anos, mdape_lgbm, "-o", color=GOLD, lw=2, label="LightGBM")
    ax2.plot(anos, mdape_ridge, "-s", color=TEAL, lw=2, label="Ridge")
    ax2.set_xlabel("ano alvo")
    ax2.set_ylabel("MdAPE (%)")
    ax2.set_title("erro percentual mediano por ano")
    ax2.set_xticks(anos)
    ax2.grid(True, alpha=0.25)
    ax2.legend()

    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def grava_metricas(con, resultados):
    con.execute("DROP TABLE IF EXISTS mb_alpha1_walkforward")
    con.execute("""
        CREATE TABLE mb_alpha1_walkforward (
            ano_alvo   INTEGER NOT NULL,
            modelo     TEXT NOT NULL,
            n_treino   INTEGER NOT NULL,
            n_teste    INTEGER NOT NULL,
            r2         REAL,
            mae_log    REAL,
            mdape_pct  REAL,
            gerado_em  TEXT,
            PRIMARY KEY (ano_alvo, modelo)
        )
    """)
    ts = datetime.utcnow().isoformat(timespec="seconds")
    rows = []
    for r in resultados:
        for modelo in ("ridge", "lgbm"):
            m = r[modelo]
            rows.append((
                r["ano"], modelo, r["n_tr"], r["n_te"],
                m["r2"], m["mae"], m["mdape"], ts,
            ))
    con.executemany(
        "INSERT INTO mb_alpha1_walkforward VALUES (?,?,?,?,?,?,?,?)", rows
    )
    con.commit()


def main():
    s17 = importar_script17()
    X, y, feat_names, anos, setores, tipos_list = s17.carregar()
    anos_arr = np.array(anos, dtype=int)
    tipos_arr = np.array(tipos_list, dtype=object)

    p(f"n = {len(y)} autos, {X.shape[1]} features")
    p(f"anos no dataset: {sorted(set(anos))}")

    resultados: list[dict] = []

    for ano_alvo in ANOS_ALVO:
        mask_tr = anos_arr < ano_alvo
        mask_te = anos_arr == ano_alvo
        n_tr = int(mask_tr.sum())
        n_te = int(mask_te.sum())
        if n_te == 0:
            p(f"[skip] ano {ano_alvo} sem amostras")
            continue
        if n_tr < 100:
            p(f"[skip] ano {ano_alvo} treino curto demais ({n_tr})")
            continue

        X_tr, y_tr = X[mask_tr], y[mask_tr]
        X_te, y_te = X[mask_te], y[mask_te]
        tipos_tr = list(tipos_arr[mask_tr])
        tipos_te = list(tipos_arr[mask_te])

        w_tr = pesos_treino(tipos_tr, tipos_te, anos_arr[mask_tr], ano_alvo - 1)

        yh_ridge, yh_lgbm = treinar_e_prever(
            X_tr, y_tr, w_tr, X_te, feat_names
        )

        r_ridge = {
            "r2": r2(y_te, yh_ridge),
            "mae": float(np.mean(np.abs(y_te - yh_ridge))),
            "mdape": mdape(y_te, yh_ridge),
        }
        r_lgbm = {
            "r2": r2(y_te, yh_lgbm),
            "mae": float(np.mean(np.abs(y_te - yh_lgbm))),
            "mdape": mdape(y_te, yh_lgbm),
        }

        p(f"\n=== ano alvo {ano_alvo}: treino {n_tr} / teste {n_te} ===")
        p(f"  Ridge    R2={r_ridge['r2']:+.3f}  MAE={r_ridge['mae']:.3f}  MdAPE={r_ridge['mdape']:.1f}%")
        p(f"  LightGBM R2={r_lgbm['r2']:+.3f}  MAE={r_lgbm['mae']:.3f}  MdAPE={r_lgbm['mdape']:.1f}%")

        plot_scatter(
            y_te, yh_lgbm,
            f"LightGBM walk-forward (treino <={ano_alvo-1}, prev {ano_alvo})",
            CHARTS / f"alpha1_walkforward_{ano_alvo}.png",
            GOLD,
        )

        resultados.append({
            "ano": ano_alvo, "n_tr": n_tr, "n_te": n_te,
            "ridge": r_ridge, "lgbm": r_lgbm,
        })

    if not resultados:
        p("nenhum ano alvo com dados suficientes")
        return

    plot_curva(resultados, CHARTS / "alpha1_walkforward_curva.png")
    p(f"\ncurva salva em alpha1_walkforward_curva.png")

    con = sqlite3.connect(DB)
    grava_metricas(con, resultados)
    con.close()
    p("[OK] metricas gravadas em mb_alpha1_walkforward")


if __name__ == "__main__":
    main()
