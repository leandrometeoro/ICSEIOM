"""
17c_walkforward_por_setor.py

Walk-forward validation por setor PNGC (Decreto 5.300/2004).

Treina um LightGBM separado para cada setor costeiro com volume suficiente:
  setor 2 - Nordeste (PI, CE, RN, PB, PE, AL, SE, BA norte)
  setor 3 - Leste    (BA sul, ES)
  setor 4 - Sudeste  (RJ, SP)

Setores 1 (Norte) e 5 (Sul) tem <20 autos oil-only — ficam como mediana
historica fallback em outro ramo, nao entram aqui.

Para cada setor, roda walk-forward:
  treino setor X ateh 2022 -> prev 2023 setor X
  treino setor X ateh 2023 -> prev 2024 setor X
  treino setor X ateh 2024 -> prev 2025 setor X

Reusa carregar() do script 17 (importado como modulo).

Saidas:
  app/static/charts/alpha1_walkforward_setor{S}_{ano}.png
  app/static/charts/alpha1_walkforward_por_setor.png  (grade comparativa)
  mb_alpha1_walkforward_setor (tabela)
"""
import importlib.util
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import lightgbm as lgb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
CHARTS = ROOT / "app" / "static" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)

NAVY = "#21295C"
GOLD = "#C8A13A"
TEAL = "#1C7293"

SETORES_NOME = {
    1: "Norte", 2: "Nordeste", 3: "Leste", 4: "Sudeste", 5: "Sul",
}
SETORES_ML = [2, 3, 4]   # com dados suficientes
ANOS_ALVO = [2023, 2024, 2025]
MIN_TREINO = 50


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
    yt = np.exp(y_true); yp = np.exp(y_pred)
    err = np.abs(yp - yt) / np.clip(yt, 1.0, None)
    return float(np.median(err) * 100.0)


def r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - y_true.mean()) ** 2))
    if ss_tot <= 0:
        return float("nan")
    return 1.0 - ss_res / ss_tot


def pesos_treino(tipos_tr, tipos_te, anos_tr, ano_ancora):
    cnt_tr = Counter(tipos_tr); cnt_te = Counter(tipos_te)
    all_tipos = set(cnt_tr) | set(cnt_te)
    n_tr, n_te = len(tipos_tr), len(tipos_te)
    K = max(len(all_tipos), 1)
    w_map = {}
    for t in all_tipos:
        ptr = (cnt_tr.get(t, 0) + 1) / (n_tr + K)
        pte = (cnt_te.get(t, 0) + 1) / (n_te + K)
        w_map[t] = float(np.clip(pte / ptr, 0.1, 10.0))
    iw = np.array([w_map[t] for t in tipos_tr], dtype=float)
    rec = np.power(2.0, (anos_tr.astype(float) - ano_ancora) / 4.0)
    w = iw * rec
    return w / w.mean()


def treina_lgbm(X_tr, y_tr, w_tr, X_te, feat_names):
    lgb_tr = lgb.Dataset(X_tr, label=y_tr, weight=w_tr, feature_name=feat_names)
    params = {
        "objective": "regression", "metric": "mae",
        "learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 30,
        "feature_fraction": 0.7, "bagging_fraction": 0.8, "bagging_freq": 5,
        "lambda_l1": 0.5, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
    }
    booster = lgb.train(params, lgb_tr, num_boost_round=300)
    return booster.predict(X_te)


def plot_scatter(y_true, y_pred, titulo, path):
    fig, ax = plt.subplots(figsize=(5.0, 5.0))
    if len(y_true) == 0:
        plt.close(fig); return
    lo = float(min(y_true.min(), y_pred.min())) - 0.5
    hi = float(max(y_true.max(), y_pred.max())) + 0.5
    ax.plot([lo, hi], [lo, hi], ls="--", lw=1.0, color="#555")
    ax.scatter(y_true, y_pred, s=30, alpha=0.6, color=GOLD, edgecolor="none")
    ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
    ax.set_xlabel("log(valor real) observado")
    ax.set_ylabel("log(valor real) previsto")
    ax.set_title(titulo)
    ax.grid(True, alpha=0.25)
    fig.tight_layout(); fig.savefig(path, dpi=140); plt.close(fig)


def plot_grade(resultados, path):
    setores_ord = sorted({r["setor"] for r in resultados})
    anos_ord = sorted({r["ano"] for r in resultados})

    fig, axes = plt.subplots(
        len(setores_ord), len(anos_ord),
        figsize=(3.6 * len(anos_ord), 3.6 * len(setores_ord)),
        squeeze=False,
    )
    for i, s in enumerate(setores_ord):
        for j, a in enumerate(anos_ord):
            ax = axes[i][j]
            r = next((x for x in resultados
                      if x["setor"] == s and x["ano"] == a), None)
            if r is None or len(r["y_te"]) == 0:
                ax.text(0.5, 0.5, "sem dados", ha="center", va="center",
                        transform=ax.transAxes, color="#888")
                ax.set_xticks([]); ax.set_yticks([])
            else:
                yt = r["y_te"]; yp = r["y_pred"]
                lo = float(min(yt.min(), yp.min())) - 0.5
                hi = float(max(yt.max(), yp.max())) + 0.5
                ax.plot([lo, hi], [lo, hi], ls="--", lw=1.0, color="#555")
                ax.scatter(yt, yp, s=22, alpha=0.6, color=GOLD,
                           edgecolor="none")
                ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
                ax.text(0.03, 0.97,
                        f"n={r['n_te']}\nR²={r['r2']:+.2f}\n"
                        f"MAE={r['mae']:.2f}\nMdAPE={r['mdape']:.0f}%",
                        transform=ax.transAxes, va="top", ha="left",
                        fontsize=8,
                        bbox=dict(boxstyle="round,pad=0.3",
                                  facecolor="white", alpha=0.85,
                                  edgecolor="#ccc"))
                ax.grid(True, alpha=0.25)
            if i == 0:
                ax.set_title(f"{a}")
            if j == 0:
                ax.set_ylabel(f"Setor {s} ({SETORES_NOME.get(s,'?')})",
                              fontsize=10)
    fig.suptitle(
        "Walk-forward por setor PNGC — LightGBM (predito × observado, log)",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(path, dpi=140); plt.close(fig)


def grava(con, resultados):
    con.execute("DROP TABLE IF EXISTS mb_alpha1_walkforward_setor")
    con.execute("""
        CREATE TABLE mb_alpha1_walkforward_setor (
            setor_pngc INTEGER NOT NULL,
            ano_alvo   INTEGER NOT NULL,
            n_treino   INTEGER NOT NULL,
            n_teste    INTEGER NOT NULL,
            r2         REAL,
            mae_log    REAL,
            mdape_pct  REAL,
            gerado_em  TEXT,
            PRIMARY KEY (setor_pngc, ano_alvo)
        )
    """)
    ts = datetime.utcnow().isoformat(timespec="seconds")
    rows = [(r["setor"], r["ano"], r["n_tr"], r["n_te"],
             r["r2"], r["mae"], r["mdape"], ts) for r in resultados]
    con.executemany(
        "INSERT INTO mb_alpha1_walkforward_setor VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    con.commit()


def main():
    s17 = importar_script17()
    X, y, feat_names, anos, setores, tipos_list = s17.carregar()
    anos_arr = np.array(anos, dtype=int)
    setor_arr = np.array(setores, dtype=int)
    tipos_arr = np.array(tipos_list, dtype=object)

    p(f"n total = {len(y)} autos")
    for s in sorted(set(setores)):
        p(f"  setor {s} ({SETORES_NOME.get(s, '?')}): {int((setor_arr==s).sum())}")

    resultados = []
    for setor in SETORES_ML:
        mask_s = setor_arr == setor
        if mask_s.sum() < 50:
            p(f"[skip] setor {setor} com poucos autos")
            continue

        p(f"\n### SETOR {setor} ({SETORES_NOME[setor]}) ###")
        for ano_alvo in ANOS_ALVO:
            mask_tr = mask_s & (anos_arr < ano_alvo)
            mask_te = mask_s & (anos_arr == ano_alvo)
            n_tr = int(mask_tr.sum())
            n_te = int(mask_te.sum())
            if n_te == 0:
                p(f"  {ano_alvo}: sem autos de teste")
                continue
            if n_tr < MIN_TREINO:
                p(f"  {ano_alvo}: treino curto ({n_tr}), skip")
                continue

            X_tr, y_tr = X[mask_tr], y[mask_tr]
            X_te, y_te = X[mask_te], y[mask_te]
            tipos_tr = list(tipos_arr[mask_tr])
            tipos_te = list(tipos_arr[mask_te])
            w_tr = pesos_treino(tipos_tr, tipos_te, anos_arr[mask_tr],
                                ano_alvo - 1)

            yh = treina_lgbm(X_tr, y_tr, w_tr, X_te, feat_names)

            m_r2 = r2(y_te, yh)
            m_mae = float(np.mean(np.abs(y_te - yh)))
            m_mdape = mdape(y_te, yh)
            p(f"  {ano_alvo}: treino={n_tr}  teste={n_te}  "
              f"R²={m_r2:+.3f}  MAE={m_mae:.3f}  MdAPE={m_mdape:.1f}%")

            plot_scatter(
                y_te, yh,
                f"Setor {setor} ({SETORES_NOME[setor]}) — "
                f"treino ≤{ano_alvo-1}, prev {ano_alvo}",
                CHARTS / f"alpha1_walkforward_setor{setor}_{ano_alvo}.png",
            )

            resultados.append({
                "setor": setor, "ano": ano_alvo,
                "n_tr": n_tr, "n_te": n_te,
                "r2": m_r2, "mae": m_mae, "mdape": m_mdape,
                "y_te": y_te, "y_pred": yh,
            })

    if not resultados:
        p("nenhum resultado")
        return

    plot_grade(resultados, CHARTS / "alpha1_walkforward_por_setor.png")
    p("\ngrade salva em alpha1_walkforward_por_setor.png")

    con = sqlite3.connect(DB)
    grava(con, resultados)
    con.close()
    p("[OK] mb_alpha1_walkforward_setor gravada")

    # Resumo final
    p("\n=== RESUMO ===")
    p(f"{'setor':<18} {'ano':>5} {'n_te':>5} {'R²':>7} {'MAE':>7} {'MdAPE':>7}")
    for r in resultados:
        p(f"{SETORES_NOME[r['setor']]:<18} {r['ano']:>5} {r['n_te']:>5} "
          f"{r['r2']:>+7.3f} {r['mae']:>7.3f} {r['mdape']:>6.1f}%")


if __name__ == "__main__":
    main()
