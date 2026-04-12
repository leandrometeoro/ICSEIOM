"""
19_export_previsoes_2025.py

Exporta para CSV os 40 autos de 2025 com valor real, previsto (walk-forward
LGBM treinado ate 2024), erro percentual e metadados. Usado como anexo de
defesa institucional ICSEIOM.

Saida:
  app/static/data/alpha1_2025_previsto_vs_real.csv
"""
import csv
import importlib.util
import math
import sqlite3
from collections import Counter
from pathlib import Path

import numpy as np
import lightgbm as lgb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
OUT = ROOT / "app" / "static" / "data" / "alpha1_2025_previsto_vs_real.csv"


def main():
    spec = importlib.util.spec_from_file_location(
        "s17", ROOT / "scripts" / "17_treinar_modelos_alpha1.py"
    )
    s17 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(s17)
    X, y, feat_names, anos, setores, tipos_list = s17.carregar()

    anos_arr = np.array(anos, dtype=int)
    tipos_arr = np.array(tipos_list, dtype=object)
    ANO = 2025
    mask_tr = anos_arr < ANO
    mask_te = anos_arr == ANO

    X_tr, y_tr = X[mask_tr], y[mask_tr]
    X_te, y_te = X[mask_te], y[mask_te]
    tipos_tr = list(tipos_arr[mask_tr])
    tipos_te = list(tipos_arr[mask_te])

    cnt_tr = Counter(tipos_tr)
    cnt_te = Counter(tipos_te)
    all_t = set(cnt_tr) | set(cnt_te)
    K = max(len(all_t), 1)
    n_tr, n_te = len(tipos_tr), len(tipos_te)
    w_map = {
        t: float(np.clip(
            ((cnt_te.get(t, 0) + 1) / (n_te + K))
            / ((cnt_tr.get(t, 0) + 1) / (n_tr + K)),
            0.1, 10.0,
        ))
        for t in all_t
    }
    iw = np.array([w_map[t] for t in tipos_tr])
    rec = np.power(2.0, (anos_arr[mask_tr].astype(float) - 2024) / 4.0)
    w = iw * rec
    w /= w.mean()

    lgb_tr = lgb.Dataset(X_tr, label=y_tr, weight=w, feature_name=feat_names)
    params = {
        "objective": "regression", "metric": "mae",
        "learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 30,
        "feature_fraction": 0.7, "bagging_fraction": 0.8, "bagging_freq": 5,
        "lambda_l1": 0.5, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
    }
    booster = lgb.train(params, lgb_tr, num_boost_round=300)
    yh = booster.predict(X_te)

    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT af.seq_auto, af.code_muni, m.nome || '/' || m.uf, af.tipo_infracao,
               af.nu_norma, af.valor_real_rs, af.tipo_multa, af.motivacao_conduta,
               af.efeito_meio_amb, af.passivel_recup
        FROM mb_alpha1_auto_feat af
        JOIN municipios_brasil m ON m.code_muni = af.code_muni
        WHERE af.log_pop IS NOT NULL AND af.log_pib_pc IS NOT NULL
          AND af.setor_pngc IS NOT NULL AND af.cluster_id IS NOT NULL
          AND af.nu_norma IN ('9966/00','4136/2002')
          AND af.ano = 2025
        ORDER BY af.seq_auto
    """).fetchall()
    con.close()

    assert len(rows) == len(y_te), f"mismatch {len(rows)} vs {len(y_te)}"

    registros = []
    for i, r in enumerate(rows):
        (seq, code, muni_uf, tipo, norma, vreal,
         tipo_multa, motivacao, efeito, passivel) = r
        pred_rs = float(math.exp(yh[i]))
        erro_abs = pred_rs - vreal
        erro_pct = erro_abs / vreal * 100.0
        registros.append({
            "seq_auto": seq,
            "municipio_uf": muni_uf,
            "tipo_infracao": tipo,
            "norma": norma,
            "tipo_multa": tipo_multa or "",
            "motivacao": motivacao or "",
            "efeito_meio_ambiente": efeito or "",
            "passivel_recuperacao": passivel or "",
            "valor_real_rs": round(vreal, 2),
            "valor_previsto_rs": round(pred_rs, 2),
            "erro_absoluto_rs": round(erro_abs, 2),
            "erro_pct": round(erro_pct, 1),
        })

    registros.sort(key=lambda d: d["valor_real_rs"], reverse=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(registros[0].keys()))
        w.writeheader()
        for r in registros:
            w.writerow(r)

    reais = np.array([d["valor_real_rs"] for d in registros])
    preds = np.array([d["valor_previsto_rs"] for d in registros])
    erros = np.abs(preds - reais) / reais * 100.0
    mdape = float(np.median(erros))
    mean_abs = float(np.mean(np.abs(preds - reais)))
    ss_res = float(np.sum((np.log(preds) - np.log(reais)) ** 2))
    y_log = np.log(reais)
    ss_tot = float(np.sum((y_log - y_log.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    print(f"[OK] {OUT.relative_to(ROOT)}")
    print(f"     n={len(registros)}  MdAPE={mdape:.1f}%  "
          f"erro_medio_abs=R$ {mean_abs:,.0f}  R2(log)={r2:+.3f}")


if __name__ == "__main__":
    main()
