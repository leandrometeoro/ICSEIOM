"""
16_estratificar_alpha1.py

Estratificacao hierarquica dos municipios costeiros para a regressao de alpha1:

  Nivel 1 (fixo):       Setor PNGC (5 setores geograficos)
  Nivel 2 (semantico):  estrato "infra" (tem_infra=1) separado como cluster_id=0.
                        Grandes portos/refinarias tem dinamica estrutural distinta
                        (multas muito maiores, mais autos) e dominam qualquer
                        clusterizacao nao-hierarquica, inflando silhouette com
                        solucoes triviais "outlier vs resto".
  Nivel 3 (data-driven): dentro do resto (tem_infra=0), PCA + GMM (fallback
                        KMeans), k otimo por silhouette.

Features por municipio (lidas de mb_features_muni):
  log_pop            — porte demografico
  log_pib_pc         — porte economico
  log_n_autos        — exposicao historica a autuacoes
  log_mediana_multa  — magnitude tipica da multa
  tem_infra          — flag infra oleo (nao entra no PCA; entra no KMeans/GMM
                        como coluna adicional apos padronizacao)

Missing values sao imputados com a mediana do setor. Padronizacao StandardScaler.
PCA retem componentes ate atingir >= 85% da variancia. Sweep de k em
[2, min(8, n_muni_setor // 10)]. GMM full-covariance quando n_muni >= 30, se
falhar usa KMeans. Desempate entre k por Davies-Bouldin (menor melhor).

Grava em mb_estratos_alpha1(code_muni, setor_pngc, cluster_id, k_setor) e
em app/static/data/diag_estratos.json para a pagina de metodologia.
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
OUT_JSON = ROOT / "app" / "static" / "data" / "diag_estratos.json"

SETOR_NOME = {1: "Norte", 2: "Nordeste", 3: "Leste", 4: "Sudeste", 5: "Sul"}

FEATURES = [
    "log_pop",
    "log_pib_pc",
    "log_n_autos",
    "log_mediana_multa",
]


def p(msg: str) -> None:
    print(msg, flush=True)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute("DROP TABLE IF EXISTS mb_estratos_alpha1")
    con.execute("""
        CREATE TABLE mb_estratos_alpha1 (
            code_muni  TEXT PRIMARY KEY,
            setor_pngc INTEGER NOT NULL,
            cluster_id INTEGER NOT NULL,
            k_setor    INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX idx_mb_estr_setor ON mb_estratos_alpha1(setor_pngc)")


def impute_median(X: np.ndarray) -> np.ndarray:
    X = X.copy()
    for j in range(X.shape[1]):
        col = X[:, j]
        mask = np.isnan(col)
        if mask.any():
            med = np.nanmedian(col) if (~mask).any() else 0.0
            X[mask, j] = med
    return X


def escolher_k(X: np.ndarray, n: int) -> tuple[int, float, float, str, np.ndarray, list[dict]]:
    """Escolhe k em [2, min(8, n//8)] maximizando silhouette, MAS rejeitando
    solucoes com cluster trivial (tamanho < max(5, 10% de n)). Isso impede que
    uns poucos outliers vencam criando um cluster de 1-4 munis com silhouette
    artificialmente alto."""
    k_max = max(2, min(8, n // 8))
    min_tamanho = max(5, int(round(0.10 * n)))
    resultados = []
    best = None
    best_fallback = None  # caso nenhum k respeite o tamanho minimo
    for k in range(2, k_max + 1):
        labels = None
        tipo = None
        try:
            if n >= 30:
                gmm = GaussianMixture(
                    n_components=k, covariance_type="full",
                    random_state=42, n_init=3, max_iter=200,
                )
                gmm.fit(X)
                labels = gmm.predict(X)
                tipo = "GMM"
        except Exception as e:
            p(f"    k={k} GMM falhou: {e}; usando KMeans")
            labels = None
        if labels is None or len(set(labels)) < 2:
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X)
            tipo = "KMeans"
        if len(set(labels)) < 2:
            continue
        try:
            sil = float(silhouette_score(X, labels))
            db = float(davies_bouldin_score(X, labels))
        except Exception:
            continue
        # tamanho minimo dos clusters
        tamanhos = np.bincount(labels)
        menor = int(tamanhos.min())
        valido = menor >= min_tamanho
        resultados.append({
            "k": k, "tipo": tipo, "silhouette": sil, "davies_bouldin": db,
            "min_cluster": menor, "valido": valido,
        })
        if valido:
            if best is None or sil > best[1] or (sil == best[1] and db < best[2]):
                best = (k, sil, db, tipo, labels)
        else:
            if best_fallback is None or sil > best_fallback[1]:
                best_fallback = (k, sil, db, tipo, labels)
    if best is None:
        # nenhum k com tamanho minimo: usa o melhor bruto, ou k=1
        if best_fallback:
            return best_fallback + (resultados,)
        labels = np.zeros(n, dtype=int)
        return 1, 0.0, 0.0, "single", labels, resultados
    return best + (resultados,)


def main():
    con = sqlite3.connect(DB)
    ensure_table(con)

    rows = con.execute("""
        SELECT code_muni, setor_pngc, log_pop, log_pib_pc, log_n_autos,
               log_mediana_multa, tem_infra
        FROM mb_features_muni
        WHERE setor_pngc IS NOT NULL
        ORDER BY setor_pngc, code_muni
    """).fetchall()
    if not rows:
        p("ERRO: mb_features_muni vazia. Rode scripts/15_computar_features.py primeiro.")
        return

    diag: dict = {
        "gerado_em": datetime.utcnow().isoformat(timespec="seconds"),
        "alvo_ipca": None,
        "features": FEATURES,
        "setores": {},
    }

    por_setor: dict[int, list] = {}
    for r in rows:
        por_setor.setdefault(r[1], []).append(r)

    total_inseridos = 0
    for setor in sorted(por_setor):
        grupo = por_setor[setor]
        nome = SETOR_NOME.get(setor, f"setor{setor}")
        n = len(grupo)
        p(f"\n== Setor {setor} {nome} ({n} munis) ==")

        # Passo 1: separa estrato "infra" (tem_infra=1) como cluster_id 0.
        # Grandes portos/refinarias tem dinamica estrutural distinta e
        # dominam qualquer clusterizacao nao-hierarquica.
        infra_codes = [r[0] for r in grupo if r[6]]
        resto = [r for r in grupo if not r[6]]
        n_infra = len(infra_codes)
        n_resto = len(resto)
        p(f"  estrato infra: {n_infra} munis (cluster_id=0)")
        p(f"  resto: {n_resto} munis para clusterizar")

        # grava estrato infra
        for code in infra_codes:
            con.execute(
                "INSERT INTO mb_estratos_alpha1 (code_muni, setor_pngc, cluster_id, k_setor) "
                "VALUES (?, ?, ?, ?)",
                (code, int(setor), 0, 0),
            )
            total_inseridos += 1

        # Passo 2: PCA + GMM/KMeans no resto, usando soh features continuas.
        if n_resto < 10:
            p(f"  resto pequeno demais, todo em cluster_id=1")
            for r in resto:
                con.execute(
                    "INSERT INTO mb_estratos_alpha1 (code_muni, setor_pngc, cluster_id, k_setor) "
                    "VALUES (?, ?, ?, ?)",
                    (r[0], int(setor), 1, 1),
                )
                total_inseridos += 1
            diag["setores"][str(setor)] = {
                "nome": nome, "n_munis": n, "n_infra": n_infra, "n_resto": n_resto,
                "k_otimo": 1, "algoritmo_escolhido": "pass-through",
                "silhouette": None, "davies_bouldin": None,
                "tamanhos_cluster": {"0": n_infra, "1": n_resto},
                "sweep": [],
            }
            continue

        codes_resto = [r[0] for r in resto]
        X_raw = np.array([
            [
                float("nan") if r[2] is None else float(r[2]),
                float("nan") if r[3] is None else float(r[3]),
                float("nan") if r[4] is None else float(r[4]),
                float("nan") if r[5] is None else float(r[5]),
            ]
            for r in resto
        ])
        X = impute_median(X_raw)
        scaler = StandardScaler()
        Xs = scaler.fit_transform(X)

        pca = PCA(n_components=min(Xs.shape[1], Xs.shape[0]))
        Xp = pca.fit_transform(Xs)
        cum = np.cumsum(pca.explained_variance_ratio_)
        n_comp = int(np.searchsorted(cum, 0.85) + 1)
        n_comp = max(2, min(n_comp, Xp.shape[1]))
        Xp = Xp[:, :n_comp]
        p(f"  PCA: {n_comp} comps, var acum = {cum[n_comp-1]:.1%}")

        k, sil, db, tipo, labels, sweep = escolher_k(Xp, n_resto)
        p(f"  k* = {k} ({tipo}), silhouette = {sil:.3f}, DB = {db:.3f}")

        # shift labels de +1 para deixar cluster_id=0 reservado ao estrato infra
        labels_final = [int(lb) + 1 for lb in labels]

        contagem = {0: n_infra}
        for lb in labels_final:
            contagem[lb] = contagem.get(lb, 0) + 1
        p(f"  tamanhos: {sorted(contagem.items())}")

        for code, lb in zip(codes_resto, labels_final):
            con.execute(
                "INSERT INTO mb_estratos_alpha1 (code_muni, setor_pngc, cluster_id, k_setor) "
                "VALUES (?, ?, ?, ?)",
                (code, int(setor), lb, int(k)),
            )
            total_inseridos += 1

        diag["setores"][str(setor)] = {
            "nome": nome,
            "n_munis": n,
            "n_infra": n_infra,
            "n_resto": n_resto,
            "pca_n_componentes": n_comp,
            "pca_variancia_explicada": [float(x) for x in pca.explained_variance_ratio_.tolist()],
            "pca_loadings": pca.components_[:n_comp].tolist(),
            "k_otimo": k,
            "algoritmo_escolhido": tipo,
            "silhouette": sil,
            "davies_bouldin": db,
            "tamanhos_cluster": {str(c): int(v) for c, v in contagem.items()},
            "sweep": sweep,
        }

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
            "icseiom_mb_estratos_alpha1",
            "Estratificacao PNGC x cluster data-driven",
            "ICSEIOM",
            "—",
            datetime.utcnow().isoformat(timespec="seconds"),
            "",
            "",
            "Estrato geografico (5 setores PNGC) x cluster intra-setor (PCA + "
            "GMM/KMeans, k escolhido por silhouette), usado como coluna de estrato "
            "na regressao de alpha1.",
            "scripts/16_estratificar_alpha1.py",
            "k varia por setor. Ver app/static/data/diag_estratos.json para "
            "silhouette, DB, loadings PCA e variancia explicada por setor.",
        ),
    )
    con.commit()
    con.close()

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(diag, indent=2, ensure_ascii=False))
    p(f"\n[OK] {total_inseridos} munis estratificados")
    p(f"     diag: {OUT_JSON}")


if __name__ == "__main__":
    main()
