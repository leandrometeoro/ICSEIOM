"""ICSEIOM Web — FastAPI + Leaflet."""
from pathlib import Path
from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .config import SECRET_KEY, APP_TITLE, APP_SHORT, ORG
from .db import query_all, query_one, get_conn
from .auth import login_user, logout_user, current_user, require_admin
from .calc import (
    atualizar_multa_evento,
    calcular_icseiom,
    get_alpha5_base,
    registrar_evento,
    set_alpha5_base,
    sugerir_multa_rs,
)

ROOT = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=ROOT / "templates")
STATIC = ROOT / "static"

app = FastAPI(title=APP_TITLE)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, same_site="lax")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def ctx(request: Request, **extra):
    return {
        "APP_TITLE": APP_TITLE,
        "APP_SHORT": APP_SHORT,
        "ORG": ORG,
        "user": current_user(request),
        **extra,
    }


def render(request: Request, name: str, **extra):
    return TEMPLATES.TemplateResponse(request, name, ctx(request, **extra))


# ────────────── Páginas públicas ──────────────

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if current_user(request):
        return RedirectResponse("/admin/dataset", status_code=303)
    from datetime import datetime
    ano = datetime.utcnow().year
    setores = api_icseiom_por_setor(ano=str(ano))
    n_eventos = query_one("SELECT COUNT(*) c FROM eventos")["c"]
    total_acum = query_one("SELECT COALESCE(SUM(icseiom_rs),0) s FROM resultados")["s"]
    return render(
        request, "publica.html",
        setores=setores, ano=ano,
        n_eventos=n_eventos, total_acum=total_acum,
    )


@app.get("/mapa-municipios", response_class=HTMLResponse)
def mapa_municipios_legacy():
    return RedirectResponse("/admin/dataset", status_code=301)


@app.get("/admin/dataset", response_class=HTMLResponse)
def admin_dataset(request: Request, user: str = Depends(require_admin)):
    anos = [r["ano"] for r in query_all(
        "SELECT DISTINCT ano FROM mb_alpha1_multa ORDER BY ano DESC"
    )]
    return render(request, "mapa_municipios.html", anos_alpha1=anos)


@app.get("/api/mb/alphas")
def api_mb_alphas(alpha: str = "soma", ano: str = "todos"):
    """Retorna valor por muni costeiro, por α individual ou soma.

    alpha:
      soma -> Σ(α₁..α₅) por muni
      a1..a5 -> α individual
    ano (apenas α₁):
      todos -> soma historica
      YYYY  -> ano especifico
    """
    def estimador_a1():
        """alpha1_hat por municipio costeiro (tabela mb_alpha1_estimativa).

        Valor esperado de multa IBAMA-oleo por auto, media geometrica do
        historico do proprio municipio (via=muni) ou fallback hierarquico
        (setor_infra -> setor -> global). Veja scripts/25.
        """
        rows = query_all(
            "SELECT e.code_muni, e.alpha1_hat AS valor_rs, e.via, e.n_base "
            "FROM mb_alpha1_estimativa e "
            "JOIN municipios_brasil m ON m.code_muni = e.code_muni "
            "WHERE m.is_costeiro = 1"
        )
        return {r["code_muni"]: (r["valor_rs"], r["via"], r["n_base"]) for r in rows}

    def media_anual(tabela: str, col: str):
        rows = query_all(
            f"SELECT code_muni, AVG({col}) AS valor_rs "
            f"FROM {tabela} GROUP BY code_muni"
        )
        return {r["code_muni"]: r["valor_rs"] for r in rows}

    mapa_tabela = {
        "a2": ("alpha2_pesca", "valor_rs"),
        "a3": ("alpha3_turismo", "vab_aloj_rs"),
        "a4": ("alpha4_saude", "custo_rs"),
        "a5": ("alpha5_ecossistemas", "valor_teeb_rs"),
    }

    if alpha == "a1":
        valores = {
            c: {"valor_rs": v, "via": via, "n_base": nb}
            for c, (v, via, nb) in estimador_a1().items()
        }
    elif alpha in mapa_tabela:
        tab, col = mapa_tabela[alpha]
        valores = {c: {"valor_rs": v} for c, v in media_anual(tab, col).items()}
    elif alpha == "soma":
        out: dict[str, float] = {}
        for c, (v, _via, _nb) in estimador_a1().items():
            out[c] = out.get(c, 0.0) + (v or 0)
        for tab, col in mapa_tabela.values():
            for c, v in media_anual(tab, col).items():
                out[c] = out.get(c, 0.0) + (v or 0)
        valores = {c: {"valor_rs": v} for c, v in out.items()}
    else:
        raise HTTPException(400, "alpha invalido")

    total = sum((d["valor_rs"] or 0) for d in valores.values())
    return {
        "alpha": alpha, "ano": ano,
        "total_rs": total, "n_municipios": len(valores),
        "valores": valores,
    }


@app.get("/api/mb/alpha1")
def api_mb_alpha1(ano: str = "todos", user: str = Depends(require_admin)):
    """Retorna agregado alpha1 por municipio costeiro.

    ano="todos" -> soma toda a serie historica por municipio.
    ano="YYYY"   -> valores do ano especifico.
    """
    if ano == "todos":
        rows = query_all(
            "SELECT a.code_muni, SUM(a.valor_rs) AS valor_rs, SUM(a.n_autos) AS n_autos "
            "FROM mb_alpha1_multa a "
            "JOIN municipios_brasil m ON m.code_muni = a.code_muni "
            "WHERE m.is_costeiro = 1 "
            "GROUP BY a.code_muni"
        )
    else:
        try:
            ano_int = int(ano)
        except ValueError:
            raise HTTPException(400, "ano invalido")
        rows = query_all(
            "SELECT a.code_muni, a.valor_rs, a.n_autos "
            "FROM mb_alpha1_multa a "
            "JOIN municipios_brasil m ON m.code_muni = a.code_muni "
            "WHERE m.is_costeiro = 1 AND a.ano = ?",
            (ano_int,),
        )
    valores = {r["code_muni"]: {"valor_rs": r["valor_rs"], "n_autos": r["n_autos"]} for r in rows}
    total = sum(v["valor_rs"] for v in valores.values())
    return {
        "ano": ano,
        "total_rs": total,
        "n_municipios": len(valores),
        "valores": valores,
    }


@app.get("/api/mb/autos/{code_muni}")
def api_mb_autos(code_muni: str, user: str = Depends(require_admin)):
    """Lista autos IBAMA individuais de um municipio (tabela mb_alpha1_autos)."""
    muni = query_one(
        "SELECT code_muni, nome, uf FROM municipios_brasil WHERE code_muni = ?",
        (code_muni,),
    )
    if not muni:
        raise HTTPException(404, "municipio nao encontrado")
    autos = query_all(
        "SELECT seq_auto, ano, mes, dt_fato, valor_rs, tipo_infracao, "
        "des_infracao, gravidade, artigo, tp_norma, nu_norma, "
        "tipo_auto, tipo_multa, efeito_meio_amb, match_via "
        "FROM mb_alpha1_autos WHERE code_muni = ? AND relevante_oleo = 1 "
        "ORDER BY ano DESC, mes DESC, seq_auto",
        (code_muni,),
    )
    total = sum((a["valor_rs"] or 0) for a in autos)
    return {
        "code_muni": muni["code_muni"],
        "nome": muni["nome"],
        "uf": muni["uf"],
        "n_autos": len(autos),
        "total_rs": total,
        "autos": [dict(a) for a in autos],
    }


@app.get("/api/mb/detalhe/{code_muni}")
def api_mb_detalhe(code_muni: str, alpha: str = "a1"):
    """Detalhe de um município para qualquer alpha ou soma."""
    muni = query_one(
        "SELECT code_muni, nome, uf FROM municipios_brasil WHERE code_muni = ?",
        (code_muni,),
    )
    if not muni:
        raise HTTPException(404, "municipio nao encontrado")

    base = {"code_muni": muni["code_muni"], "nome": muni["nome"], "uf": muni["uf"]}

    if alpha == "a1":
        autos = query_all(
            "SELECT seq_auto, ano, mes, dt_fato, valor_rs, tipo_infracao, "
            "des_infracao, gravidade, artigo, tp_norma, nu_norma, "
            "tipo_auto, tipo_multa, efeito_meio_amb, match_via "
            "FROM mb_alpha1_autos WHERE code_muni = ? AND relevante_oleo = 1 "
            "ORDER BY ano DESC, mes DESC, seq_auto",
            (code_muni,),
        )
        total = sum((a["valor_rs"] or 0) for a in autos)
        est = query_one(
            "SELECT alpha1_hat, via, n_base FROM mb_alpha1_estimativa WHERE code_muni = ?",
            (code_muni,),
        )
        return {**base, "alpha": "a1", "n_rows": len(autos), "total_rs": total,
                "estimativa": dict(est) if est else None,
                "rows": [dict(a) for a in autos]}

    if alpha == "a2":
        rows = query_all(
            "SELECT ano, valor_rs, toneladas, fonte "
            "FROM alpha2_pesca WHERE code_muni = ? ORDER BY ano DESC",
            (code_muni,),
        )
        total = sum((r["valor_rs"] or 0) for r in rows)
        return {**base, "alpha": "a2", "n_rows": len(rows), "total_rs": total,
                "rows": [dict(r) for r in rows]}

    if alpha == "a3":
        rows = query_all(
            "SELECT ano, vab_aloj_rs AS valor_rs, fonte "
            "FROM alpha3_turismo WHERE code_muni = ? ORDER BY ano DESC",
            (code_muni,),
        )
        total = sum((r["valor_rs"] or 0) for r in rows)
        return {**base, "alpha": "a3", "n_rows": len(rows), "total_rs": total,
                "rows": [dict(r) for r in rows]}

    if alpha == "a4":
        rows = query_all(
            "SELECT ano, custo_rs AS valor_rs, n_internacoes, fonte "
            "FROM alpha4_saude WHERE code_muni = ? ORDER BY ano DESC",
            (code_muni,),
        )
        total = sum((r["valor_rs"] or 0) for r in rows)
        return {**base, "alpha": "a4", "n_rows": len(rows), "total_rs": total,
                "rows": [dict(r) for r in rows]}

    if alpha == "a5":
        rows = query_all(
            "SELECT ano, valor_teeb_rs AS valor_rs, ha_manguezal, ha_recife, "
            "ha_restinga, fonte "
            "FROM alpha5_ecossistemas WHERE code_muni = ? ORDER BY ano DESC",
            (code_muni,),
        )
        total = sum((r["valor_rs"] or 0) for r in rows)
        return {**base, "alpha": "a5", "n_rows": len(rows), "total_rs": total,
                "rows": [dict(r) for r in rows]}

    if alpha == "soma":
        resumo = []
        labels = {"a1": "α₁ multa", "a2": "α₂ pesca", "a3": "α₃ turismo",
                  "a4": "α₄ saúde", "a5": "α₅ ecossistemas"}
        est = query_one(
            "SELECT alpha1_hat FROM mb_alpha1_estimativa WHERE code_muni = ?",
            (code_muni,),
        )
        if est:
            resumo.append({"alpha": "a1", "label": labels["a1"],
                           "valor_rs": est["alpha1_hat"]})
        for alpha_k, tab, col in [
            ("a2", "alpha2_pesca", "valor_rs"),
            ("a3", "alpha3_turismo", "vab_aloj_rs"),
            ("a4", "alpha4_saude", "custo_rs"),
            ("a5", "alpha5_ecossistemas", "valor_teeb_rs"),
        ]:
            r = query_one(
                f"SELECT {col} AS valor_rs FROM {tab} "
                f"WHERE code_muni = ? ORDER BY ano DESC LIMIT 1",
                (code_muni,),
            )
            if r and r["valor_rs"]:
                resumo.append({"alpha": alpha_k, "label": labels[alpha_k],
                               "valor_rs": r["valor_rs"]})
        total = sum(r["valor_rs"] for r in resumo)
        return {**base, "alpha": "soma", "n_rows": len(resumo), "total_rs": total,
                "rows": resumo}

    raise HTTPException(400, "alpha invalido")


@app.get("/metodologia", response_class=HTMLResponse)
def metodologia(request: Request):
    fontes = query_all(
        "SELECT fonte, nome_humano, orgao, ultima_safra, url, url_portal, "
        "descricao_uso, observacoes_metodologicas, atualizado_em, script "
        "FROM metadados_atualizacao "
        "ORDER BY COALESCE(orgao, ''), COALESCE(nome_humano, fonte)"
    )
    return render(request, "metodologia.html", fontes=fontes)


@app.get("/historico", response_class=HTMLResponse)
def historico(request: Request):
    eventos = query_all(
        "SELECT e.id_evento, e.data_evento, e.lon, e.lat, e.raio_km, e.foi_poluente, "
        "e.descricao, r.icseiom_rs, r.k_aplicado, "
        "r.alpha1_rs, r.alpha2_rs, r.alpha3_rs, r.alpha4_rs, r.alpha5_rs "
        "FROM eventos e LEFT JOIN resultados r ON r.id_evento = e.id_evento "
        "ORDER BY e.data_evento DESC, e.id_evento DESC"
    )
    rotulos = {
        "alpha1_rs": "α₁ multa",
        "alpha2_rs": "α₂ pesca",
        "alpha3_rs": "α₃ turismo",
        "alpha4_rs": "α₄ saúde",
        "alpha5_rs": "α₅ ecossistemas",
    }
    for e in eventos:
        pares = [(k, e.get(k) or 0) for k in rotulos]
        maior = max(pares, key=lambda p: p[1])
        e["setor_dominante"] = rotulos[maior[0]] if maior[1] > 0 else "—"
    return render(request, "historico.html", eventos=eventos)


@app.get("/evento/{id_evento}", response_class=HTMLResponse)
def evento_detalhe(request: Request, id_evento: int):
    ev = query_one(
        "SELECT e.*, r.* FROM eventos e LEFT JOIN resultados r ON r.id_evento = e.id_evento "
        "WHERE e.id_evento = ?",
        (id_evento,),
    )
    if not ev:
        raise HTTPException(404, "Evento não encontrado")
    muns = query_all(
        "SELECT em.code_muni, em.fracao, m.nome, m.uf, m.lat_centro, m.lon_centro "
        "FROM eventos_municipios em "
        "JOIN municipios_costeiros m ON m.code_muni = em.code_muni "
        "WHERE em.id_evento = ? ORDER BY em.fracao DESC",
        (id_evento,),
    )
    return render(request, "evento.html", ev=ev, muns=muns)


# ────────────── API pública (JSON) ──────────────

def _wkt_polygon_to_coords(wkt: str) -> list[list[list[float]]] | None:
    """Converte POLYGON((x y, x y, ...)) WKT em anel GeoJSON [[[x,y], ...]]."""
    if not wkt:
        return None
    s = wkt.strip()
    if not s.upper().startswith("POLYGON"):
        return None
    i = s.find("((")
    j = s.rfind("))")
    if i < 0 or j < 0:
        return None
    ring = []
    for par in s[i + 2:j].split(","):
        xy = par.strip().split()
        if len(xy) >= 2:
            try:
                ring.append([float(xy[0]), float(xy[1])])
            except ValueError:
                return None
    if len(ring) < 3:
        return None
    return [ring]


@app.get("/api/municipios_br.geojson")
def municipios_br_geojson(costeiros: int = 0, user: str = Depends(require_admin)):
    """Serve a malha IBGE completa (5570 munis) ou apenas os costeiros.

    Enriquece cada feature com sum_alpha quando o muni tiver dados em
    alpha*_* tables. Cache em memoria no primeiro request.
    """
    import json
    from pathlib import Path
    global _MALHA_CACHE, _MALHA_COSTEIRA_CACHE
    path = Path(__file__).resolve().parent / "static" / "data" / "municipios_br.geojson"
    if "_MALHA_CACHE" not in globals() or _MALHA_CACHE is None:
        with path.open("r", encoding="utf-8") as f:
            _MALHA_CACHE = json.load(f)
        somas = {
            r["code_muni"]: r["sum_alpha"] for r in query_all(
                "SELECT m.code_muni, "
                "COALESCE(a1.valor_rs,0) + COALESCE(a2.valor_rs,0) + "
                "COALESCE(a3.vab_aloj_rs,0) + COALESCE(a4.custo_rs,0) + "
                "COALESCE(a5.valor_teeb_rs,0) AS sum_alpha "
                "FROM municipios_costeiros m "
                "LEFT JOIN alpha1_multa_ambiental a1 ON a1.code_muni=m.code_muni "
                "LEFT JOIN alpha2_pesca a2 ON a2.code_muni=m.code_muni "
                "LEFT JOIN alpha3_turismo a3 ON a3.code_muni=m.code_muni "
                "LEFT JOIN alpha4_saude a4 ON a4.code_muni=m.code_muni "
                "LEFT JOIN alpha5_ecossistemas a5 ON a5.code_muni=m.code_muni"
            )
        }
        costeiros_db = {
            r["code_muni"] for r in query_all(
                "SELECT code_muni FROM municipios_brasil WHERE is_costeiro=1"
            )
        }
        for feat in _MALHA_CACHE["features"]:
            code = feat["properties"]["code_muni"]
            feat["properties"]["sum_alpha"] = somas.get(code, 0)
            feat["properties"]["is_costeiro"] = 1 if code in costeiros_db else 0
        _MALHA_COSTEIRA_CACHE = {
            "type": "FeatureCollection",
            "features": [f for f in _MALHA_CACHE["features"]
                         if f["properties"].get("is_costeiro")],
        }
    return _MALHA_COSTEIRA_CACHE if costeiros else _MALHA_CACHE


@app.get("/api/municipios.geojson")
def municipios_geojson():
    rows = query_all(
        "SELECT m.code_muni, m.nome, m.uf, m.regiao, m.pop_2022, m.lat_centro, m.lon_centro, "
        "m.geom_wkt, "
        "COALESCE(a1.valor_rs,0) + COALESCE(a2.valor_rs,0) + COALESCE(a3.vab_aloj_rs,0) + "
        "COALESCE(a4.custo_rs,0) + COALESCE(a5.valor_teeb_rs,0) AS sum_alpha "
        "FROM municipios_costeiros m "
        "LEFT JOIN alpha1_multa_ambiental a1 ON a1.code_muni=m.code_muni "
        "LEFT JOIN alpha2_pesca a2 ON a2.code_muni=m.code_muni "
        "LEFT JOIN alpha3_turismo a3 ON a3.code_muni=m.code_muni "
        "LEFT JOIN alpha4_saude a4 ON a4.code_muni=m.code_muni "
        "LEFT JOIN alpha5_ecossistemas a5 ON a5.code_muni=m.code_muni"
    )
    features = []
    for r in rows:
        coords = _wkt_polygon_to_coords(r["geom_wkt"])
        if coords:
            geom = {"type": "Polygon", "coordinates": coords}
        else:
            geom = {"type": "Point",
                    "coordinates": [r["lon_centro"], r["lat_centro"]]}
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "code_muni": r["code_muni"],
                "nome": r["nome"],
                "uf": r["uf"],
                "regiao": r["regiao"],
                "pop_2022": r["pop_2022"],
                "sum_alpha": r["sum_alpha"],
                "lat_centro": r["lat_centro"],
                "lon_centro": r["lon_centro"],
            },
        })
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/eventos")
def api_eventos():
    rows = query_all(
        "SELECT e.id_evento, e.data_evento, e.lon, e.lat, e.raio_km, e.foi_poluente, "
        "e.descricao, e.valor_multa_rs, e.multa_provisoria, "
        "r.alpha1_rs, r.icseiom_rs "
        "FROM eventos e LEFT JOIN resultados r ON r.id_evento = e.id_evento "
        "ORDER BY e.data_evento DESC"
    )
    afetados: dict[int, list[dict]] = {}
    for r in query_all(
        "SELECT em.id_evento, em.code_muni, em.fracao, m.nome, m.uf "
        "FROM eventos_municipios em "
        "LEFT JOIN municipios_costeiros m ON m.code_muni = em.code_muni"
    ):
        afetados.setdefault(r["id_evento"], []).append({
            "code_muni": r["code_muni"],
            "fracao": r["fracao"],
            "nome": r["nome"],
            "uf": r["uf"],
        })
    out = []
    for r in rows:
        d = dict(r)
        d["municipios_afetados"] = afetados.get(r["id_evento"], [])
        out.append(d)
    return out


@app.get("/api/fontes")
def api_fontes():
    return query_all("SELECT * FROM metadados_atualizacao ORDER BY fonte")


@app.get("/api/icseiom-por-setor")
def api_icseiom_por_setor(ano: str = "todos"):
    """Agrega por setor (α₁..α₅) o valor atribuido à LGAF (× k_aplicado).

    ano="todos" soma toda a serie; ano="YYYY" filtra por ano do evento.
    Retorna totais por setor e por evento.
    """
    sql = (
        "SELECT r.alpha1_rs, r.alpha2_rs, r.alpha3_rs, r.alpha4_rs, r.alpha5_rs, "
        "r.k_aplicado, substr(e.data_evento,1,4) AS ano "
        "FROM resultados r JOIN eventos e ON e.id_evento=r.id_evento"
    )
    params: tuple = ()
    if ano != "todos":
        try:
            ano_int = int(ano)
        except ValueError:
            raise HTTPException(400, "ano invalido")
        sql += " WHERE substr(e.data_evento,1,4)=?"
        params = (str(ano_int),)
    rows = query_all(sql, params)
    setores = {"multa": 0.0, "pesca": 0.0, "turismo": 0.0, "saude": 0.0, "ecossistemas": 0.0}
    for r in rows:
        k = r["k_aplicado"] or 0
        setores["multa"] += (r["alpha1_rs"] or 0) * k
        setores["pesca"] += (r["alpha2_rs"] or 0) * k
        setores["turismo"] += (r["alpha3_rs"] or 0) * k
        setores["saude"] += (r["alpha4_rs"] or 0) * k
        setores["ecossistemas"] += (r["alpha5_rs"] or 0) * k
    total = sum(setores.values())
    return {
        "ano": ano,
        "n_eventos": len(rows),
        "total_rs": round(total, 2),
        "setores": {k: round(v, 2) for k, v in setores.items()},
    }


# ────────────── Autenticação ──────────────

@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request, erro: str | None = None):
    return render(request, "login.html", erro=erro)


@app.post("/login")
def login_submit(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    if login_user(request, username, password):
        return RedirectResponse("/admin", status_code=303)
    return RedirectResponse("/login?erro=1", status_code=303)


@app.get("/logout")
def logout(request: Request):
    logout_user(request)
    return RedirectResponse("/", status_code=303)


# ────────────── Admin ──────────────

@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request, user: str = Depends(require_admin)):
    fontes = query_all("SELECT * FROM metadados_atualizacao ORDER BY fonte")
    n_mun = query_one("SELECT COUNT(*) c FROM municipios_costeiros")["c"]
    n_ev = query_one("SELECT COUNT(*) c FROM eventos")["c"]
    total = query_one("SELECT COALESCE(SUM(icseiom_rs),0) s FROM resultados")["s"]
    return render(request, "admin/dashboard.html", fontes=fontes, n_mun=n_mun, n_ev=n_ev, total=total)


@app.get("/admin/novo-evento", response_class=HTMLResponse)
def novo_evento_form(request: Request, user: str = Depends(require_admin)):
    return render(request, "admin/novo_evento.html")


@app.post("/admin/novo-evento")
def novo_evento_submit(
    request: Request,
    user: str = Depends(require_admin),
    data_evento: str = Form(...),
    lat: float = Form(...),
    lon: float = Form(...),
    raio_km: float = Form(...),
    foi_poluente: str = Form("nao"),
    descricao: str = Form(""),
    valor_multa_rs: str = Form(""),
    beta_override_rs: str = Form(""),
    chi_override_rs: str = Form(""),
):
    poluente = foi_poluente == "sim"

    def _parse_rs(s: str) -> float | None:
        if not s or not s.strip():
            return None
        try:
            return float(s.replace(",", "."))
        except ValueError:
            return None

    multa_val = _parse_rs(valor_multa_rs)
    beta_ov = _parse_rs(beta_override_rs)
    chi_ov = _parse_rs(chi_override_rs)
    id_ev = registrar_evento(
        data_evento, lon, lat, raio_km, poluente, descricao,
        valor_multa_rs=multa_val, multa_provisoria=True,
        beta_override_rs=beta_ov, chi_override_rs=chi_ov,
    )
    return RedirectResponse(f"/evento/{id_ev}", status_code=303)


@app.get("/api/sugerir-multa")
def api_sugerir_multa(lat: float, lon: float, raio_km: float):
    return {"valor_rs": sugerir_multa_rs(lon, lat, raio_km)}


@app.post("/api/preview-evento")
async def api_preview_evento(request: Request, user: str = Depends(require_admin)):
    """Calcula ICSEIOM sem persistir. Reaproveita calcular_icseiom()."""
    body = await request.json()
    try:
        data_evento = str(body["data_evento"])
        lon = float(body["lon"])
        lat = float(body["lat"])
        raio_km = float(body["raio_km"])
    except (KeyError, TypeError, ValueError):
        raise HTTPException(400, "parametros obrigatorios: data_evento, lon, lat, raio_km")
    poluente = bool(body.get("foi_poluente", False))
    multa_val = body.get("valor_multa_rs")
    if isinstance(multa_val, str):
        multa_val = float(multa_val.replace(",", ".")) if multa_val.strip() else None
    beta_override = body.get("beta_override_rs")
    chi_override = body.get("chi_override_rs")
    if isinstance(beta_override, str):
        beta_override = float(beta_override.replace(",", ".")) if beta_override.strip() else None
    if isinstance(chi_override, str):
        chi_override = float(chi_override.replace(",", ".")) if chi_override.strip() else None
    res = calcular_icseiom(
        data_evento, lon, lat, raio_km, poluente, multa_val,
        beta_override_rs=beta_override, chi_override_rs=chi_override,
    )
    return {
        "ano_safra": res.ano_safra,
        "alpha1_rs": res.alpha1_rs,
        "alpha2_rs": res.alpha2_rs,
        "alpha3_rs": res.alpha3_rs,
        "alpha4_rs": res.alpha4_rs,
        "alpha5_rs": res.alpha5_rs,
        "beta_rs": res.beta_rs,
        "chi_rs": res.chi_rs,
        "k": res.k,
        "icseiom_rs": res.icseiom_rs,
        "municipios": res.municipios,
    }


@app.post("/admin/evento/{id_evento}/corrigir-multa")
def admin_corrigir_multa(
    id_evento: int,
    user: str = Depends(require_admin),
    valor_multa_rs: float = Form(...),
    provisoria: str = Form("nao"),
):
    atualizar_multa_evento(
        id_evento, valor_multa_rs, provisoria == "sim",
    )
    return RedirectResponse(f"/evento/{id_evento}", status_code=303)


CHI_CATEGORIAS = ["pessoal", "insumos", "fixos"]
BETA_CATEGORIAS = ["convenios", "servicos", "outros"]


@app.get("/admin/fontes", response_class=HTMLResponse)
def admin_fontes(request: Request, user: str = Depends(require_admin)):
    fontes = query_all("SELECT * FROM metadados_atualizacao ORDER BY fonte")
    chi_rows = query_all(
        "SELECT ano, categoria, valor_rs, descricao, fonte "
        "FROM chi_custos_cat ORDER BY ano DESC, categoria"
    )
    beta_rows = query_all(
        "SELECT ano, categoria, valor_rs, descricao, fonte "
        "FROM beta_receitas_cat ORDER BY ano DESC, categoria"
    )
    return render(
        request, "admin/fontes.html",
        fontes=fontes, alpha5_base=get_alpha5_base(),
        chi_rows=chi_rows, beta_rows=beta_rows,
        chi_categorias=CHI_CATEGORIAS, beta_categorias=BETA_CATEGORIAS,
    )


@app.post("/admin/custos")
def admin_custos_upsert(
    user: str = Depends(require_admin),
    tipo: str = Form(...),
    ano: int = Form(...),
    categoria: str = Form(...),
    valor_rs: float = Form(...),
    descricao: str = Form(""),
    fonte: str = Form(""),
):
    if tipo == "chi":
        if categoria not in CHI_CATEGORIAS:
            raise HTTPException(400, "categoria invalida para chi")
        tabela = "chi_custos_cat"
    elif tipo == "beta":
        if categoria not in BETA_CATEGORIAS:
            raise HTTPException(400, "categoria invalida para beta")
        tabela = "beta_receitas_cat"
    else:
        raise HTTPException(400, "tipo deve ser chi ou beta")
    con = get_conn()
    con.execute(
        f"INSERT INTO {tabela} (ano, categoria, valor_rs, descricao, fonte) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT(ano, categoria) DO UPDATE SET "
        "valor_rs=excluded.valor_rs, descricao=excluded.descricao, fonte=excluded.fonte",
        (ano, categoria, valor_rs, descricao, fonte),
    )
    con.commit()
    con.close()
    return RedirectResponse("/admin/fontes", status_code=303)


@app.post("/admin/custos/excluir")
def admin_custos_excluir(
    user: str = Depends(require_admin),
    tipo: str = Form(...),
    ano: int = Form(...),
    categoria: str = Form(...),
):
    tabela = "chi_custos_cat" if tipo == "chi" else "beta_receitas_cat"
    con = get_conn()
    con.execute(
        f"DELETE FROM {tabela} WHERE ano=? AND categoria=?", (ano, categoria)
    )
    con.commit()
    con.close()
    return RedirectResponse("/admin/fontes", status_code=303)


@app.post("/admin/alpha5-base")
def admin_alpha5_base(
    request: Request,
    user: str = Depends(require_admin),
    base: str = Form(...),
):
    """Troca a base de valoracao do alpha5 e recalcula todos os eventos."""
    if base not in ("global", "brasil"):
        raise HTTPException(400, "base invalida")
    set_alpha5_base(base)
    return RedirectResponse("/admin/fontes", status_code=303)


@app.post("/admin/fontes/upload")
def admin_fontes_upload(
    request: Request,
    user: str = Depends(require_admin),
    fonte: str = Form(...),
    ultima_safra: str = Form(...),
    observacoes: str = Form(""),
):
    from datetime import datetime
    con = get_conn()
    con.execute(
        "INSERT INTO metadados_atualizacao (fonte, ultima_safra, atualizado_em, observacoes) "
        "VALUES (?, ?, ?, ?) ON CONFLICT(fonte) DO UPDATE SET "
        "ultima_safra=excluded.ultima_safra, atualizado_em=excluded.atualizado_em, "
        "observacoes=excluded.observacoes",
        (fonte, ultima_safra, datetime.utcnow().isoformat(), observacoes),
    )
    con.commit()
    con.close()
    return RedirectResponse("/admin/fontes", status_code=303)


# Healthcheck para Docker
@app.get("/health")
def health():
    return {"status": "ok", "app": APP_SHORT}
