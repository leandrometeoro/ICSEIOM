"""
17_treinar_modelos_alpha1.py

Treina e compara dois modelos para prever log(valor_medio_real) por celula
(code_muni, ano) da mb_auto_features:

  (A) Ridge (linear, interpretavel)
  (B) LightGBM (gradient boosting, mais flexivel)

Target:
  y = log_valor_real  (multa media por auto, em R$ corrigidos por IPCA)

Features:
  Numericas
    ano                    — tendencia temporal
    log_pop                — log(populacao no ano do fato)
    log_pib_pc             — log(PIB per capita real)
    log_n_autos            — log(1 + n_autos na celula)
  Categoricas (one-hot)
    setor_pngc             — 5 setores
    cluster_id             — cluster intra-setor (inclui estrato infra=0)
  Binarias
    tem_refinaria, tem_terminal, tem_duto, tem_campo_eep

Validacao:
  Split primario TEMPORAL: treino 2005-2022, teste 2023-2026.
  Sanity: KFold(5) random sobre o treino (sem vazar o teste).

Metricas reportadas:
  R^2 (log)
  MAE (log)
  RMSE (log)
  MdAPE (Median Absolute Percentage Error, escala original)
  Spearman rho (log)

Artefatos gravados:
  db: mb_alpha1_modelos — uma linha por (modelo, split) com metricas
  app/static/data/alpha1_modelo_ridge.json — coeficientes + intercepto + feature names
  app/static/data/alpha1_modelo_lgbm.txt   — modelo booster LightGBM
  app/static/data/alpha1_modelo_meta.json  — metadados (features, encoders, etc.)
  app/static/charts/alpha1_pred_vs_obs_ridge.png
  app/static/charts/alpha1_pred_vs_obs_lgbm.png
  app/static/charts/alpha1_residuos_ano.png
  app/static/charts/alpha1_residuos_setor.png
  app/static/charts/alpha1_feat_importance_lgbm.png
  app/static/charts/alpha1_ridge_coefs.png
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler
from scipy.stats import spearmanr

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
CHARTS = ROOT / "app" / "static" / "charts"
DATA = ROOT / "app" / "static" / "data"
CHARTS.mkdir(parents=True, exist_ok=True)
DATA.mkdir(parents=True, exist_ok=True)

ANO_CORTE = 2023  # treino < ANO_CORTE, teste >= ANO_CORTE
ANO_MIN_REGIME_ATUAL = 2020  # filtro para "era IBAMA atual" (Qualidade Ambiental
                             # passa a dominar; regime pre-2020 foi descartado
                             # apos diagnostico de drift)

NAVY = "#21295C"
DEEP = "#065A82"
TEAL = "#1C7293"
GOLD = "#C8A13A"


def p(msg: str) -> None:
    print(msg, flush=True)


def carregar() -> tuple[np.ndarray, np.ndarray, list[str], list[int], list[int], list[str]]:
    """Carrega features PER-AUTO de mb_alpha1_auto_feat.

    Target = log(valor_real_rs) do auto individual.
    Features:
      continuas: log_pop, log_pib_pc
      one-hot tipo_infracao (top-K + 'outros')
      one-hot nu_norma (fixas)
      one-hot match_via (enq, tipo, kw)
      one-hot setor_pngc (5)
      one-hot cluster_id (0..3)
      binarias infra (4)
    """
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT f.seq_auto, f.code_muni, f.ano, f.log_valor_real,
               f.tipo_infracao, f.nu_norma, f.match_via,
               f.log_pop, f.log_pib_pc, f.setor_pngc, f.cluster_id,
               f.tem_refinaria, f.tem_terminal, f.tem_duto, f.tem_campo_eep,
               f.anp_n_incidentes, f.anp_vol_oleo_m3, f.anp_vol_max_m3,
               f.anp_muni_n_total, f.anp_muni_vol_total, f.anp_muni_vol_max,
               f.tipo_multa, f.motivacao_conduta, f.efeito_meio_amb, f.efeito_saude,
               f.passivel_recup, f.qt_area, f.classificacao_area, f.tipo_acao
        FROM mb_alpha1_auto_feat f
        JOIN mb_alpha1_autos a ON a.seq_auto = f.seq_auto
        WHERE f.log_pop IS NOT NULL AND f.log_pib_pc IS NOT NULL
          AND f.setor_pngc IS NOT NULL AND f.cluster_id IS NOT NULL
          AND a.relevante_oleo = 1
    """).fetchall()
    con.close()

    # descobre categorias presentes
    tipos_cnt: dict[str, int] = {}
    normas_cnt: dict[str, int] = {}
    for r in rows:
        tipo = (r[4] or "outros").strip() or "outros"
        norma = (r[5] or "sem").strip() or "sem"
        tipos_cnt[tipo] = tipos_cnt.get(tipo, 0) + 1
        normas_cnt[norma] = normas_cnt.get(norma, 0) + 1

    # tipos com >=30 obs; o resto vira "outros"
    tipos_ok = [t for t, c in tipos_cnt.items() if c >= 30]
    # normas com >=30 obs
    normas_ok = [n for n, c in normas_cnt.items() if c >= 30]
    p(f"  tipos_infracao one-hot: {len(tipos_ok)}")
    p(f"  normas one-hot: {len(normas_ok)}")

    vias = ["enq", "tipo", "kw"]

    X_list: list[list[float]] = []
    y_list: list[float] = []
    anos: list[int] = []
    setores: list[int] = []

    import math
    tipos_list: list[str] = []
    for r in rows:
        (_, _, ano, y, tipo, norma, via,
         log_pop, log_pib_pc, setor, cluster,
         ref, term, duto, eep,
         anp_n, anp_vol, anp_max,
         anp_mn, anp_mv, anp_mx,
         tipo_multa, motivacao, efeito_meio, efeito_saude,
         passivel, qt_area, class_area, tipo_acao) = r

        tipo = (tipo or "outros").strip() or "outros"
        norma = (norma or "sem").strip() or "sem"

        log_ano = math.log(max(int(ano) - 2004, 1))
        log_anp_n = math.log1p(float(anp_n or 0))
        log_anp_vol = math.log1p(float(anp_vol or 0))
        log_anp_max = math.log1p(float(anp_max or 0))
        log_anp_muni_n = math.log1p(float(anp_mn or 0))
        log_anp_muni_vol = math.log1p(float(anp_mv or 0))
        log_anp_muni_max = math.log1p(float(anp_mx or 0))
        log_qt_area = math.log1p(float(qt_area or 0))

        feat: list[float] = [
            log_ano,
            float(log_pop), float(log_pib_pc),
            float(ref), float(term), float(duto), float(eep),
            log_anp_n, log_anp_vol, log_anp_max,
            log_anp_muni_n, log_anp_muni_vol, log_anp_muni_max,
            log_qt_area,
        ]
        # setor 1..5
        for s in (1, 2, 3, 4, 5):
            feat.append(1.0 if setor == s else 0.0)
        # cluster 0..3
        for c in (0, 1, 2, 3):
            feat.append(1.0 if cluster == c else 0.0)
        # tipo_infracao
        for t in tipos_ok:
            feat.append(1.0 if tipo == t else 0.0)
        feat.append(1.0 if tipo not in tipos_ok else 0.0)  # "outros"
        # norma
        for nn in normas_ok:
            feat.append(1.0 if norma == nn else 0.0)
        feat.append(1.0 if norma not in normas_ok else 0.0)  # "outras"
        # via
        for v in vias:
            feat.append(1.0 if via == v else 0.0)

        # SICAFI categoricas novas — listas fixas com catchall
        tm = (tipo_multa or "").strip()
        for v in ("Aberta", "Fechada"):
            feat.append(1.0 if tm == v else 0.0)
        feat.append(1.0 if tm not in ("Aberta", "Fechada") else 0.0)

        mv = (motivacao or "").strip()
        for v in ("Intencional", "Não intencional"):
            feat.append(1.0 if mv == v else 0.0)
        feat.append(1.0 if mv not in ("Intencional", "Não intencional") else 0.0)

        em = (efeito_meio or "").strip()
        for v in ("Desprezível", "Fraca", "Moderada", "Potencial", "Significativa"):
            feat.append(1.0 if em == v else 0.0)
        feat.append(1.0 if em not in ("Desprezível", "Fraca", "Moderada",
                                      "Potencial", "Significativa") else 0.0)

        es = (efeito_saude or "").strip()
        for v in ("Desprezível", "Fraca", "Potencial", "Significativa"):
            feat.append(1.0 if es == v else 0.0)
        feat.append(1.0 if es not in ("Desprezível", "Fraca", "Potencial",
                                      "Significativa") else 0.0)

        pr = (passivel or "").strip().upper()
        feat.append(1.0 if pr == "S" else 0.0)
        feat.append(1.0 if pr == "N" else 0.0)
        feat.append(1.0 if pr not in ("S", "N") else 0.0)

        ta = (tipo_acao or "").strip()
        for v in ("Operação", "Rotina", "Fortuito"):
            feat.append(1.0 if ta == v else 0.0)
        feat.append(1.0 if ta not in ("Operação", "Rotina", "Fortuito") else 0.0)

        ca = (class_area or "").strip()
        for v in ("Fluvial/Marítimo", "Amazônia Legal", "Atividade"):
            feat.append(1.0 if ca == v else 0.0)
        feat.append(1.0 if ca not in ("Fluvial/Marítimo", "Amazônia Legal",
                                      "Atividade") else 0.0)

        X_list.append(feat)
        y_list.append(float(y))
        anos.append(int(ano))
        setores.append(int(setor))
        tipos_list.append(tipo)

    def slug_feat(s: str, prefix: str, idx: int) -> str:
        import re, unicodedata
        s2 = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        s2 = re.sub(r"[^A-Za-z0-9]+", "_", s2).strip("_").lower()[:24]
        return f"{prefix}_{idx:02d}_{s2}" if s2 else f"{prefix}_{idx:02d}"

    feat_names = [
        "log_ano",
        "log_pop", "log_pib_pc",
        "tem_refinaria", "tem_terminal", "tem_duto", "tem_campo_eep",
        "log_anp_n_incidentes", "log_anp_vol_oleo", "log_anp_vol_max",
        "log_anp_muni_n", "log_anp_muni_vol", "log_anp_muni_max",
        "log_qt_area",
        "setor_1_norte", "setor_2_nordeste", "setor_3_leste",
        "setor_4_sudeste", "setor_5_sul",
        "cluster_0_infra", "cluster_1", "cluster_2", "cluster_3",
    ] + [slug_feat(t, "tipo", i) for i, t in enumerate(tipos_ok)] + ["tipo_catchall"] \
      + [slug_feat(n, "norma", i) for i, n in enumerate(normas_ok)] + ["norma_catchall"] \
      + [f"via_{v}" for v in vias] \
      + ["tm_aberta", "tm_fechada", "tm_na"] \
      + ["mot_intencional", "mot_nao_intencional", "mot_na"] \
      + ["em_desprezivel", "em_fraca", "em_moderada", "em_potencial",
         "em_significativa", "em_na"] \
      + ["es_desprezivel", "es_fraca", "es_potencial", "es_significativa",
         "es_na"] \
      + ["passivel_s", "passivel_n", "passivel_na"] \
      + ["acao_operacao", "acao_rotina", "acao_fortuito", "acao_na"] \
      + ["ca_fluvial_mar", "ca_amazonia", "ca_atividade", "ca_na"]
    X = np.array(X_list, dtype=float)
    y = np.array(y_list, dtype=float)

    # Drop APENAS features puramente constantes (nnz=0) no dataset filtrado.
    # Sao housekeeping — testamos drop mais agressivo (nnz<10) e piorou o R2.
    FEATURES_DROP = {
        "log_qt_area",     # nnz=0 no dataset filtrado
        "via_tipo",        # nnz=0
        "ca_atividade",    # nnz=0
        "cluster_3",       # nnz=0
    }
    keep_idx = [i for i, n in enumerate(feat_names) if n not in FEATURES_DROP]
    X = X[:, keep_idx]
    feat_names = [feat_names[i] for i in keep_idx]
    p(f"  features apos drop: {len(feat_names)} (removidas {len(FEATURES_DROP)})")
    return X, y, feat_names, anos, setores, tipos_list


def metricas(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    r2 = float(r2_score(y_true, y_pred))
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    # MdAPE na escala original: exp(y) vs exp(y_pred)
    yt = np.exp(y_true)
    yp = np.exp(y_pred)
    mdape = float(np.median(np.abs(yt - yp) / np.maximum(yt, 1.0)))
    rho, _ = spearmanr(y_true, y_pred)
    return {
        "r2": r2, "mae_log": mae, "rmse_log": rmse,
        "mdape": mdape, "spearman": float(rho),
    }


def plot_pred_vs_obs(y_true, y_pred, titulo, path, cor):
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(y_true, y_pred, s=12, alpha=0.5, color=cor)
    lo = min(y_true.min(), y_pred.min())
    hi = max(y_true.max(), y_pred.max())
    ax.plot([lo, hi], [lo, hi], "--", color="#888", lw=1)
    ax.set_xlabel("log(valor real) observado")
    ax.set_ylabel("log(valor real) previsto")
    ax.set_title(titulo)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_residuos_por(grupo_vals, residuos, titulo, xlabel, path, cor):
    unicos = sorted(set(grupo_vals))
    data = [np.array([r for g, r in zip(grupo_vals, residuos) if g == u]) for u in unicos]
    fig, ax = plt.subplots(figsize=(8, 4))
    bp = ax.boxplot(data, labels=[str(u) for u in unicos], patch_artist=True,
                    showmeans=True, meanline=True)
    for patch in bp["boxes"]:
        patch.set_facecolor(cor)
        patch.set_alpha(0.5)
    ax.axhline(0, color="#555", lw=1, ls="--")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("residuo (log obs - log pred)")
    ax.set_title(titulo)
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_feat_importance(names, values, titulo, path, cor):
    order = np.argsort(values)[::-1]
    names_s = [names[i] for i in order]
    vals_s = [values[i] for i in order]
    fig, ax = plt.subplots(figsize=(8, max(4, 0.3 * len(names))))
    ax.barh(range(len(names_s)), vals_s, color=cor)
    ax.set_yticks(range(len(names_s)))
    ax.set_yticklabels(names_s)
    ax.invert_yaxis()
    ax.set_title(titulo)
    ax.grid(alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute("DROP TABLE IF EXISTS mb_alpha1_modelos")
    con.execute("""
        CREATE TABLE mb_alpha1_modelos (
            modelo    TEXT NOT NULL,
            split     TEXT NOT NULL,
            n         INTEGER NOT NULL,
            r2        REAL,
            mae_log   REAL,
            rmse_log  REAL,
            mdape     REAL,
            spearman  REAL,
            treinado_em TEXT NOT NULL,
            PRIMARY KEY (modelo, split)
        )
    """)


def grava_metricas(con, modelo, split, n, m):
    con.execute(
        "INSERT INTO mb_alpha1_modelos VALUES (?,?,?,?,?,?,?,?,?)",
        (modelo, split, n, m["r2"], m["mae_log"], m["rmse_log"],
         m["mdape"], m["spearman"], datetime.utcnow().isoformat(timespec="seconds")),
    )


def main():
    if not HAS_LGB:
        p("AVISO: lightgbm nao instalado, rodando soh Ridge")

    p("carregando dados...")
    X, y, feat_names, anos, setores, tipos_all = carregar()
    p(f"  n = {len(y)} celulas, {X.shape[1]} features")

    anos_arr = np.array(anos)

    # Dois splits:
    #   (a) aleatorio 80/20: avalia a capacidade preditiva pura
    #       (dado tipo/norma/porte, qual o valor esperado da multa).
    #   (b) temporal 2023+: diagnostica drift de distribuicao.
    rng = np.random.default_rng(42)
    n_total = len(y)
    idx_all = np.arange(n_total)
    rng.shuffle(idx_all)
    n_train = int(0.8 * n_total)
    idx_tr_rand = np.sort(idx_all[:n_train])
    idx_te_rand = np.sort(idx_all[n_train:])

    X_tr_r, y_tr_r = X[idx_tr_rand], y[idx_tr_rand]
    X_te_r, y_te_r = X[idx_te_rand], y[idx_te_rand]

    # Split temporal (mesmo do diagnostico anterior)
    mask_tr = anos_arr < ANO_CORTE
    mask_te = ~mask_tr
    X_tr, y_tr = X[mask_tr], y[mask_tr]
    X_te, y_te = X[mask_te], y[mask_te]
    setores_te = [s for s, m in zip(setores, mask_te) if m]
    anos_te = anos_arr[mask_te]
    tipos_tr = [t for t, m in zip(tipos_all, mask_tr) if m]
    tipos_te = [t for t, m in zip(tipos_all, mask_te) if m]
    p(f"  split aleatorio: train {len(y_tr_r)} / test {len(y_te_r)}")
    p(f"  split temporal : train {len(y_tr)} (< {ANO_CORTE}) / test {len(y_te)} (>= {ANO_CORTE})")

    # === Importance weighting combinado: tipo_infracao * recency exponencial ===
    # IW_tipo corrige drift de composicao; recency up-pesa anos recentes.
    from collections import Counter
    cnt_tr = Counter(tipos_tr)
    cnt_te = Counter(tipos_te)
    n_tr, n_te = len(tipos_tr), len(tipos_te)
    all_tipos = set(cnt_tr) | set(cnt_te)
    K = max(len(all_tipos), 1)
    w_map: dict[str, float] = {}
    for t in all_tipos:
        p_tr = (cnt_tr.get(t, 0) + 1) / (n_tr + K)
        p_te = (cnt_te.get(t, 0) + 1) / (n_te + K)
        w_map[t] = float(np.clip(p_te / p_tr, 0.1, 10.0))
    iw_tipo = np.array([w_map[t] for t in tipos_tr], dtype=float)
    # recency: peso dobra a cada 4 anos. Ano 2022 ~= 1.0, 2018 ~= 0.5, 2010 ~= 0.12
    anos_tr_np = anos_arr[mask_tr].astype(float)
    iw_rec = np.power(2.0, (anos_tr_np - 2022) / 4.0)
    w_tr = iw_tipo * iw_rec
    w_tr = w_tr / w_tr.mean()
    p(f"  IW final: min={w_tr.min():.2f} max={w_tr.max():.2f} mean={w_tr.mean():.2f}")
    top_w = sorted(w_map.items(), key=lambda x: -x[1])[:5]
    p(f"  IW tipo top: " + ", ".join(f"{t}={w:.2f}" for t, w in top_w))

    con = sqlite3.connect(DB)
    ensure_table(con)

    # ============================================================
    # PASSADA 1 — SPLIT ALEATORIO 80/20 (avaliacao primaria)
    # ============================================================
    p("\n=== SPLIT ALEATORIO 80/20 ===")
    scaler_r = StandardScaler()
    Xs_tr_r = scaler_r.fit_transform(X_tr_r)
    Xs_te_r = scaler_r.transform(X_te_r)

    ridge_r = Ridge(alpha=1.0, random_state=42).fit(Xs_tr_r, y_tr_r)
    yhat_rand_ridge = ridge_r.predict(Xs_te_r)
    m_rand_ridge = metricas(y_te_r, yhat_rand_ridge)
    p(f"  Ridge    test: R2={m_rand_ridge['r2']:.3f} MAE={m_rand_ridge['mae_log']:.3f} "
      f"MdAPE={m_rand_ridge['mdape']:.1%} rho={m_rand_ridge['spearman']:.3f}")
    grava_metricas(con, "ridge", "random20_test", len(y_te_r), m_rand_ridge)

    if HAS_LGB:
        lgb_tr_r = lgb.Dataset(X_tr_r, label=y_tr_r, feature_name=feat_names)
        params_r = {
            "objective": "regression", "metric": "mae",
            "learning_rate": 0.05, "num_leaves": 31,
            "min_data_in_leaf": 20, "feature_fraction": 0.9,
            "bagging_fraction": 0.9, "bagging_freq": 5,
            "verbose": -1, "seed": 42,
        }
        booster_r = lgb.train(params_r, lgb_tr_r, num_boost_round=500)
        yhat_rand_lgbm = booster_r.predict(X_te_r)
        m_rand_lgbm = metricas(y_te_r, yhat_rand_lgbm)
        p(f"  LightGBM test: R2={m_rand_lgbm['r2']:.3f} MAE={m_rand_lgbm['mae_log']:.3f} "
          f"MdAPE={m_rand_lgbm['mdape']:.1%} rho={m_rand_lgbm['spearman']:.3f}")
        grava_metricas(con, "lightgbm", "random20_test", len(y_te_r), m_rand_lgbm)

        # salva o booster treinado no split aleatorio (modelo de producao)
        booster_r.save_model(str(DATA / "alpha1_modelo_lgbm_random.txt"))
        plot_pred_vs_obs(y_te_r, yhat_rand_lgbm,
                         "LightGBM - split aleatorio 80/20",
                         CHARTS / "alpha1_pred_vs_obs_lgbm_random.png", GOLD)

    plot_pred_vs_obs(y_te_r, yhat_rand_ridge,
                     "Ridge - split aleatorio 80/20",
                     CHARTS / "alpha1_pred_vs_obs_ridge_random.png", DEEP)

    p("\n=== SPLIT TEMPORAL 2005-2022 vs 2023-2026 (diagnostico de drift) ===")

    # === Baseline: mediana por (setor_pngc, cluster_id) ===
    # Referencia honesta: o quanto que a estratificacao sozinha explica.
    # Indices das colunas one-hot de setor e cluster em feat_names
    p("\nbaseline: mediana log(y) por estrato (setor, cluster)...")
    def estrato_key(row_x):
        setor = None
        for i, s in enumerate((1, 2, 3, 4, 5), start=feat_names.index("setor_1_norte")):
            if row_x[i] == 1.0:
                setor = s; break
        cluster = None
        for i, c in enumerate((0, 1, 2, 3), start=feat_names.index("cluster_0_infra")):
            if row_x[i] == 1.0:
                cluster = c; break
        return (setor, cluster)

    estrato_medianas: dict = {}
    por_estrato: dict = {}
    for row_x, yv in zip(X_tr, y_tr):
        k = estrato_key(row_x)
        por_estrato.setdefault(k, []).append(yv)
    for k, vals in por_estrato.items():
        estrato_medianas[k] = float(np.median(vals))
    media_global_tr = float(np.median(y_tr))

    def baseline_predict(X_any):
        out = []
        for row_x in X_any:
            k = estrato_key(row_x)
            out.append(estrato_medianas.get(k, media_global_tr))
        return np.array(out)

    yhat_tr_b = baseline_predict(X_tr)
    yhat_te_b = baseline_predict(X_te)
    m_tr_b = metricas(y_tr, yhat_tr_b)
    m_te_b = metricas(y_te, yhat_te_b)
    p(f"  train: R2={m_tr_b['r2']:.3f} MAE_log={m_tr_b['mae_log']:.3f} MdAPE={m_tr_b['mdape']:.1%}")
    p(f"  test : R2={m_te_b['r2']:.3f} MAE_log={m_te_b['mae_log']:.3f} MdAPE={m_te_b['mdape']:.1%}")
    grava_metricas(con, "baseline_estrato", "train", len(y_tr), m_tr_b)
    grava_metricas(con, "baseline_estrato", "test", len(y_te), m_te_b)

    # === Baseline 2: mediana por municipio (hist. pre-corte) com fallback ===
    p("\nbaseline: mediana log(y) por municipio (fallback estrato)...")
    por_muni_tr: dict = {}
    # precisa dos codes dos rows de treino — refazer query
    con2 = sqlite3.connect(DB)
    rows2 = con2.execute("""
        SELECT code_muni, ano, log_valor_real, setor_pngc, cluster_id
        FROM mb_alpha1_auto_feat
        WHERE log_pop IS NOT NULL AND log_pib_pc IS NOT NULL
          AND setor_pngc IS NOT NULL AND cluster_id IS NOT NULL
        ORDER BY code_muni, ano
    """).fetchall()
    con2.close()
    muni_tr_hist: dict[str, list[float]] = {}
    code_anoteste: list[tuple[str, float, tuple]] = []
    for code, ano_row, yv, setor_row, cluster_row in rows2:
        if ano_row < ANO_CORTE:
            muni_tr_hist.setdefault(code, []).append(yv)
        else:
            code_anoteste.append((code, yv, (setor_row, cluster_row)))
    muni_medianas = {c: float(np.median(v)) for c, v in muni_tr_hist.items()}

    y_te2 = []
    yhat_te2 = []
    n_hit_muni = 0
    n_hit_estr = 0
    for code, yv, stk in code_anoteste:
        if code in muni_medianas:
            yp = muni_medianas[code]
            n_hit_muni += 1
        else:
            yp = estrato_medianas.get(stk, media_global_tr)
            n_hit_estr += 1
        y_te2.append(yv); yhat_te2.append(yp)
    y_te2 = np.array(y_te2); yhat_te2 = np.array(yhat_te2)
    m_te_m = metricas(y_te2, yhat_te2)
    p(f"  test : R2={m_te_m['r2']:.3f} MAE_log={m_te_m['mae_log']:.3f} MdAPE={m_te_m['mdape']:.1%}")
    p(f"  cobertura muni historico: {n_hit_muni}/{len(code_anoteste)} ({n_hit_muni/max(len(code_anoteste),1):.0%})")
    grava_metricas(con, "baseline_muni", "test", len(y_te2), m_te_m)

    # === Ridge ===
    p("\ntreinando Ridge...")
    scaler = StandardScaler()
    Xs_tr = scaler.fit_transform(X_tr)
    Xs_te = scaler.transform(X_te)

    ridge = Ridge(alpha=1.0, random_state=42)
    ridge.fit(Xs_tr, y_tr, sample_weight=w_tr)

    yhat_tr_r = ridge.predict(Xs_tr)
    yhat_te_r = ridge.predict(Xs_te)
    m_tr_r = metricas(y_tr, yhat_tr_r)
    m_te_r = metricas(y_te, yhat_te_r)
    p(f"  train: R2={m_tr_r['r2']:.3f} MAE_log={m_tr_r['mae_log']:.3f} MdAPE={m_tr_r['mdape']:.1%}")
    p(f"  test : R2={m_te_r['r2']:.3f} MAE_log={m_te_r['mae_log']:.3f} MdAPE={m_te_r['mdape']:.1%}")

    # 5-fold CV no treino (sanity)
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    r2_cv = []
    for tr_idx, va_idx in kf.split(Xs_tr):
        r_cv = Ridge(alpha=1.0).fit(Xs_tr[tr_idx], y_tr[tr_idx])
        r2_cv.append(r2_score(y_tr[va_idx], r_cv.predict(Xs_tr[va_idx])))
    p(f"  5fold CV R2 = {np.mean(r2_cv):.3f} +- {np.std(r2_cv):.3f}")

    grava_metricas(con, "ridge", "train", len(y_tr), m_tr_r)
    grava_metricas(con, "ridge", "test", len(y_te), m_te_r)
    grava_metricas(con, "ridge", "cv5_mean", len(y_tr),
                   {"r2": float(np.mean(r2_cv)), "mae_log": 0, "rmse_log": 0,
                    "mdape": 0, "spearman": 0})

    # salva Ridge
    ridge_json = {
        "feature_names": feat_names,
        "scaler_mean": scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "coef": ridge.coef_.tolist(),
        "intercept": float(ridge.intercept_),
        "alpha": 1.0,
        "target": "log_valor_real",
    }
    (DATA / "alpha1_modelo_ridge.json").write_text(json.dumps(ridge_json, indent=2))

    plot_pred_vs_obs(y_te, yhat_te_r, "Ridge - teste 2023-2026",
                     CHARTS / "alpha1_pred_vs_obs_ridge.png", DEEP)

    # coef plot (em escala padronizada, ja interpretavel como importancia)
    plot_feat_importance(feat_names, np.abs(ridge.coef_),
                         "Ridge - |coeficiente| padronizado",
                         CHARTS / "alpha1_ridge_coefs.png", TEAL)

    # === LightGBM ===
    if HAS_LGB:
        p("\ntreinando LightGBM (regularizado + IW recency)...")
        lgb_tr = lgb.Dataset(X_tr, label=y_tr, weight=w_tr, feature_name=feat_names)
        params = {
            "objective": "regression",
            "metric": "mae",
            "learning_rate": 0.03,
            "num_leaves": 15,
            "min_data_in_leaf": 60,
            "feature_fraction": 0.7,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "lambda_l1": 0.5,
            "lambda_l2": 1.0,
            "verbose": -1,
            "seed": 42,
        }
        booster = lgb.train(params, lgb_tr, num_boost_round=300)

        yhat_tr_g = booster.predict(X_tr)
        yhat_te_g = booster.predict(X_te)
        m_tr_g = metricas(y_tr, yhat_tr_g)
        m_te_g = metricas(y_te, yhat_te_g)
        p(f"  train: R2={m_tr_g['r2']:.3f} MAE_log={m_tr_g['mae_log']:.3f} MdAPE={m_tr_g['mdape']:.1%}")
        p(f"  test : R2={m_te_g['r2']:.3f} MAE_log={m_te_g['mae_log']:.3f} MdAPE={m_te_g['mdape']:.1%}")

        # CV
        r2_cv_g = []
        for tr_idx, va_idx in kf.split(X_tr):
            bd = lgb.Dataset(X_tr[tr_idx], label=y_tr[tr_idx], feature_name=feat_names)
            bo = lgb.train(params, bd, num_boost_round=500)
            r2_cv_g.append(r2_score(y_tr[va_idx], bo.predict(X_tr[va_idx])))
        p(f"  5fold CV R2 = {np.mean(r2_cv_g):.3f} +- {np.std(r2_cv_g):.3f}")

        grava_metricas(con, "lightgbm", "train", len(y_tr), m_tr_g)
        grava_metricas(con, "lightgbm", "test", len(y_te), m_te_g)
        grava_metricas(con, "lightgbm", "cv5_mean", len(y_tr),
                       {"r2": float(np.mean(r2_cv_g)), "mae_log": 0, "rmse_log": 0,
                        "mdape": 0, "spearman": 0})

        booster.save_model(str(DATA / "alpha1_modelo_lgbm.txt"))
        plot_pred_vs_obs(y_te, yhat_te_g, "LightGBM - teste 2023-2026",
                         CHARTS / "alpha1_pred_vs_obs_lgbm.png", GOLD)

        imp = booster.feature_importance(importance_type="gain")
        plot_feat_importance(feat_names, imp.astype(float),
                             "LightGBM - importancia (gain)",
                             CHARTS / "alpha1_feat_importance_lgbm.png", NAVY)

        # residuos combinados (usa o melhor = lgbm se bateu ridge)
        resid_te = y_te - yhat_te_g
        plot_residuos_por(list(anos_te), resid_te,
                          "Residuos LightGBM por ano (teste)", "ano",
                          CHARTS / "alpha1_residuos_ano.png", GOLD)
        plot_residuos_por(setores_te, resid_te,
                          "Residuos LightGBM por setor PNGC (teste)", "setor",
                          CHARTS / "alpha1_residuos_setor.png", TEAL)
    else:
        resid_te = y_te - yhat_te_r
        plot_residuos_por(list(anos_te), resid_te,
                          "Residuos Ridge por ano (teste)", "ano",
                          CHARTS / "alpha1_residuos_ano.png", DEEP)
        plot_residuos_por(setores_te, resid_te,
                          "Residuos Ridge por setor PNGC (teste)", "setor",
                          CHARTS / "alpha1_residuos_setor.png", TEAL)

    # === Estimador "honesto" para consumo pelo calc.py ===
    # Dado o piso de explicacao baixo (~10%), o estimador recomendado
    # AGORA e a mediana por estrato (setor, cluster) no log-espaco,
    # convertida de volta para R$ via exp(). Ate ingerirmos per-auto com
    # tipo_infracao/norma/artigo, nenhum modelo sofisticado bate isso no
    # hold-out. Ver mb_alpha1_modelos.
    estimador = {
        "gerado_em": datetime.utcnow().isoformat(timespec="seconds"),
        "tipo": "mediana_por_estrato",
        "unidade": "R$ corrigidos IPCA (mes-alvo via scripts/15)",
        "mediana_global_rs": float(np.exp(media_global_tr)),
        "estratos": [
            {
                "setor_pngc": k[0],
                "cluster_id": k[1],
                "n_obs": len(por_estrato[k]),
                "mediana_log": float(estrato_medianas[k]),
                "mediana_rs": float(np.exp(estrato_medianas[k])),
            }
            for k in sorted(por_estrato.keys(), key=lambda x: (x[0] or 0, x[1] or 0))
        ],
        "nota": "Modelos Ridge e LightGBM foram treinados para comparacao "
                "mas ambos underperformam o baseline no hold-out temporal "
                "porque os features agregados (porte, setor, cluster, infra) "
                "nao capturam os discriminadores reais da multa. Re-ingerir "
                "IBAMA preservando auto-nivel com tipo_infracao/norma/artigo "
                "e o proximo passo.",
    }
    (DATA / "alpha1_estimador.json").write_text(
        json.dumps(estimador, indent=2, ensure_ascii=False)
    )

    # metadados
    meta = {
        "gerado_em": datetime.utcnow().isoformat(timespec="seconds"),
        "target": "log_valor_real",
        "features": feat_names,
        "n_total": int(len(y)),
        "n_treino": int(len(y_tr)),
        "n_teste": int(len(y_te)),
        "ano_corte": ANO_CORTE,
        "modelos": ["ridge"] + (["lightgbm"] if HAS_LGB else []),
    }
    (DATA / "alpha1_modelo_meta.json").write_text(json.dumps(meta, indent=2))

    # registra fonte
    con.execute(
        "INSERT INTO metadados_atualizacao "
        "(fonte, nome_humano, orgao, ultima_safra, atualizado_em, url, url_portal, "
        "descricao_uso, script, observacoes_metodologicas) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(fonte) DO UPDATE SET "
        "nome_humano=excluded.nome_humano, orgao=excluded.orgao, "
        "ultima_safra=excluded.ultima_safra, atualizado_em=excluded.atualizado_em, "
        "descricao_uso=excluded.descricao_uso, script=excluded.script, "
        "observacoes_metodologicas=excluded.observacoes_metodologicas",
        (
            "icseiom_mb_alpha1_modelos",
            "Modelos alpha1 (Ridge + LightGBM)",
            "ICSEIOM",
            f"treino<{ANO_CORTE}, teste>={ANO_CORTE}",
            datetime.utcnow().isoformat(timespec="seconds"),
            "",
            "",
            "Regressao de log(valor_medio_real) por celula (muni, ano). Usada "
            "para estimar E[valor_multa] no ramo B de alpha1 (poluente nao "
            "confirmado).",
            "scripts/17_treinar_modelos_alpha1.py",
            "Hold-out temporal. Features: ano, log(pop), log(pib_pc), log(n_autos), "
            "4 flags infra, setor PNGC e cluster intra-setor. Ver "
            "app/static/charts/alpha1_*.png e mb_alpha1_modelos.",
        ),
    )

    con.commit()
    con.close()
    p("\n[OK] modelos treinados e artefatos salvos")


if __name__ == "__main__":
    main()
