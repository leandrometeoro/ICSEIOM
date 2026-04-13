"""Wrapper do calcular_icseiom que usa o path do banco da config."""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional
from .db import get_conn


def _km(lon1, lat1, lon2, lat2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


@dataclass
class ResultadoICSEIOM:
    id_evento: Optional[int]
    municipios: list
    ano_safra: int
    alpha1_rs: float
    alpha2_rs: float
    alpha3_rs: float
    alpha4_rs: float
    alpha5_rs: float
    beta_rs: float
    chi_rs: float
    k: float
    icseiom_rs: float
    beta_cat: dict
    chi_cat: dict
    beta_override: bool
    chi_override: bool


def calcular_icseiom(
    data_evento: str,
    lon: float,
    lat: float,
    raio_km: float,
    foi_poluente: bool = True,
    valor_multa_rs: float | None = None,
    beta_override_rs: float | None = None,
    chi_override_rs: float | None = None,
) -> ResultadoICSEIOM:
    con = get_conn()
    cur = con.cursor()

    ano = int(data_evento[:4])
    row = cur.execute(
        "SELECT MAX(ano) FROM alpha1_multa_ambiental WHERE ano <= ?", (ano,)
    ).fetchone()
    ano_safra = row[0] or ano

    muns = cur.execute(
        "SELECT code_muni, nome, uf, lon_centro, lat_centro FROM municipios_costeiros"
    ).fetchall()
    afetados = []
    for r in muns:
        code, nome, uf, lon_c, lat_c = r["code_muni"], r["nome"], r["uf"], r["lon_centro"], r["lat_centro"]
        d = _km(lon, lat, lon_c, lat_c)
        if d <= raio_km:
            frac = max(0.0, 1 - d / raio_km) if raio_km > 0 else 1.0
            afetados.append((code, nome, uf, d, frac))

    if not afetados:
        con.close()
        return ResultadoICSEIOM(
            None, [], ano_safra, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            beta_cat={}, chi_cat={},
            beta_override=beta_override_rs is not None,
            chi_override=chi_override_rs is not None,
        )

    def soma(tabela, col):
        # Cada alpha tem disponibilidade temporal diferente (alpha1 ate 2024,
        # alpha3 so 2021, alpha4 2023 etc). Usa o ano mais recente <= ano do
        # evento disponivel por municipio em cada tabela.
        total = 0.0
        for code, _, _, _, frac in afetados:
            v = cur.execute(
                f"SELECT {col} FROM {tabela} "
                f"WHERE code_muni=? AND ano<=? "
                f"ORDER BY ano DESC LIMIT 1",
                (code, ano),
            ).fetchone()
            total += (v[0] if v else 0) * frac
        return total

    # alpha1: valor da multa IBAMA-oleo associada ao evento.
    #
    # Ramo A (poluidor, derrame confirmado): o LGAF contribuiu para a
    # aplicacao da multa (sem analise forense nao ha prova). alpha1 = valor
    # real da multa aplicada (se informado pelo operador) ou estimativa
    # alpha1_hat do municipio (chute provisorio, corrigivel depois).
    #
    # Ramo B (nao-poluidor, evento evitado/contido): o LGAF contribuiu
    # para evitar multa indevida ou demonstrou ausencia de derrame.
    # alpha1 = valor esperado da multa que seria aplicada, estimado como
    # alpha1_hat (media geometrica historica) de cada municipio no raio,
    # ponderado pela fracao de impacto.
    #
    # Em ambos os ramos, se nenhum valor e fornecido, usa-se a estimativa
    # de mb_alpha1_estimativa (script 25).
    def alpha1_estimado():
        total = 0.0
        for code, _, _, _, frac in afetados:
            v = cur.execute(
                "SELECT alpha1_hat FROM mb_alpha1_estimativa WHERE code_muni=?",
                (code,),
            ).fetchone()
            total += (v[0] if v else 0) * frac
        return total

    if foi_poluente:
        a1 = float(valor_multa_rs) if valor_multa_rs else alpha1_estimado()
    else:
        a1 = alpha1_estimado()

    a2 = soma("alpha2_pesca", "valor_rs")
    a3 = soma("alpha3_turismo", "vab_aloj_rs")
    # alpha4 saude: so conta se o evento foi poluente. O custo de saude e
    # evitado porque a LGAF identificou o poluente e a populacao foi avisada,
    # evitando contato. Em alarmes falsos (nao-poluente), nao ha custo
    # sanitario a evitar.
    a4 = soma("alpha4_saude", "custo_rs") if foi_poluente else 0.0
    a5 = soma("alpha5_ecossistemas", "valor_teeb_rs")

    # beta/chi: prioriza tabelas por categoria (ano, categoria, valor_rs);
    # se nao houver categorias, cai no agregado anual legado.
    beta_cat = {r["categoria"]: r["valor_rs"] for r in cur.execute(
        "SELECT categoria, valor_rs FROM beta_receitas_cat WHERE ano=?", (ano_safra,)
    ).fetchall()}
    if beta_cat:
        beta = sum(beta_cat.values())
    else:
        beta_row = cur.execute(
            "SELECT valor_rs FROM beta_receitas_lgaf WHERE ano=?", (ano_safra,)
        ).fetchone()
        beta = beta_row[0] if beta_row else 0.0

    chi_cat = {r["categoria"]: r["valor_rs"] for r in cur.execute(
        "SELECT categoria, valor_rs FROM chi_custos_cat WHERE ano=?", (ano_safra,)
    ).fetchall()}
    if chi_cat:
        chi = sum(chi_cat.values())
    else:
        chi_row = cur.execute(
            "SELECT valor_rs FROM chi_custos_lgaf WHERE ano=?", (ano_safra,)
        ).fetchone()
        chi = chi_row[0] if chi_row else 0.0

    # k = share LGAF no merito coletivo do lucro social evitado. O ICSEIOM nao
    # e 100% atribuivel a LGAF: outros atores (IBAMA, ICMBio, Marinha, Defesa
    # Civil, municipios) participam da resposta e dividem o lucro evitado.
    # Valor 0,30 e placeholder ate calibracao Delphi. Ver parametros.descricao.
    k_row = cur.execute("SELECT valor FROM parametros WHERE chave='k'").fetchone()
    k = k_row[0] if k_row else 0.30

    rateio = 10
    if beta_override_rs is not None:
        beta_ev = float(beta_override_rs)
    else:
        beta_ev = beta / rateio
    if chi_override_rs is not None:
        chi_ev = float(chi_override_rs)
    else:
        chi_ev = chi / rateio

    icseiom = k * (a1 + a2 + a3 + a4 + a5) + beta_ev - chi_ev

    con.close()
    return ResultadoICSEIOM(
        id_evento=None,
        municipios=[
            {"code": c, "nome": n, "uf": u, "dist_km": round(d, 2), "frac": round(f, 3)}
            for c, n, u, d, f in afetados
        ],
        ano_safra=ano_safra,
        alpha1_rs=round(a1, 2),
        alpha2_rs=round(a2, 2),
        alpha3_rs=round(a3, 2),
        alpha4_rs=round(a4, 2),
        alpha5_rs=round(a5, 2),
        beta_rs=round(beta_ev, 2),
        chi_rs=round(chi_ev, 2),
        k=k,
        icseiom_rs=round(icseiom, 2),
        beta_cat={c: round(v, 2) for c, v in beta_cat.items()},
        chi_cat={c: round(v, 2) for c, v in chi_cat.items()},
        beta_override=beta_override_rs is not None,
        chi_override=chi_override_rs is not None,
    )


def sugerir_multa_rs(lon: float, lat: float, raio_km: float) -> float:
    """Sugere um valor de α₁ baseado no estimador α̂₁ (média geométrica
    histórica) dos municípios afetados, ponderado por fração de impacto."""
    con = get_conn()
    cur = con.cursor()
    muns = cur.execute(
        "SELECT code_muni, lon_centro, lat_centro FROM municipios_costeiros"
    ).fetchall()
    total = 0.0
    for r in muns:
        d = _km(lon, lat, r["lon_centro"], r["lat_centro"])
        if d > raio_km:
            continue
        frac = max(0.0, 1 - d / raio_km) if raio_km > 0 else 1.0
        v = cur.execute(
            "SELECT alpha1_hat FROM mb_alpha1_estimativa WHERE code_muni=?",
            (r["code_muni"],),
        ).fetchone()
        total += (v[0] if v else 0) * frac
    con.close()
    return round(total, 2)


def registrar_evento(
    data_evento: str,
    lon: float,
    lat: float,
    raio_km: float,
    foi_poluente: bool,
    descricao: str = "",
    valor_multa_rs: float | None = None,
    multa_provisoria: bool = True,
    beta_override_rs: float | None = None,
    chi_override_rs: float | None = None,
) -> int:
    """Calcula o ICSEIOM, persiste em eventos/eventos_municipios/resultados e retorna id."""
    res = calcular_icseiom(
        data_evento, lon, lat, raio_km, foi_poluente, valor_multa_rs,
        beta_override_rs=beta_override_rs, chi_override_rs=chi_override_rs,
    )
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO eventos (data_evento, lon, lat, raio_km, foi_poluente, "
        "descricao, valor_multa_rs, multa_provisoria, beta_override_rs, chi_override_rs) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (data_evento, lon, lat, raio_km, int(foi_poluente), descricao,
         valor_multa_rs, int(multa_provisoria) if foi_poluente else 0,
         beta_override_rs, chi_override_rs),
    )
    id_evento = cur.lastrowid
    for m in res.municipios:
        cur.execute(
            "INSERT INTO eventos_municipios (id_evento, code_muni, fracao) VALUES (?, ?, ?)",
            (id_evento, m["code"], m["frac"]),
        )
    cur.execute(
        "INSERT INTO resultados (id_evento, alpha1_rs, alpha2_rs, alpha3_rs, alpha4_rs, "
        "alpha5_rs, beta_rs, chi_rs, k_aplicado, icseiom_rs) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (id_evento, res.alpha1_rs, res.alpha2_rs, res.alpha3_rs, res.alpha4_rs,
         res.alpha5_rs, res.beta_rs, res.chi_rs, res.k, res.icseiom_rs),
    )
    con.commit()
    con.close()
    return id_evento


def get_alpha5_base() -> str:
    """Retorna a base ativa de valoracao do alpha5 ('global'|'brasil')."""
    con = get_conn(); cur = con.cursor()
    r = cur.execute(
        "SELECT valor_texto FROM parametros WHERE chave='alpha5_base'"
    ).fetchone()
    con.close()
    return (r[0] if r and r[0] else "global")


def set_alpha5_base(base: str) -> None:
    """Troca a base ativa de valoracao do alpha5 e reescreve valor_teeb_rs
    na tabela alpha5_ecossistemas a partir da coluna correspondente, para
    que o calculo de eventos (que usa valor_teeb_rs) passe a refletir a
    nova base imediatamente. Tambem recalcula resultados ja persistidos
    para que o historico fique consistente com a base escolhida."""
    if base not in ("global", "brasil"):
        raise ValueError(f"base invalida: {base} (esperado 'global' ou 'brasil')")
    con = get_conn(); cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")
    cur.execute(
        "INSERT INTO parametros (chave, valor, valor_texto, unidade, descricao) "
        "VALUES ('alpha5_base', 0, ?, '', "
        "'Base ativa de valoracao TEEB do alpha5') "
        "ON CONFLICT(chave) DO UPDATE SET valor_texto=excluded.valor_texto",
        (base,),
    )
    col = "valor_teeb_global_rs" if base == "global" else "valor_teeb_brasil_rs"
    cur.execute(
        f"UPDATE alpha5_ecossistemas SET valor_teeb_rs = COALESCE({col}, valor_teeb_rs)"
    )
    # Reaplica em eventos ja persistidos: recalcula alpha5/icseiom de cada evento.
    eventos = cur.execute(
        "SELECT id_evento, data_evento, lon, lat, raio_km, foi_poluente, "
        "valor_multa_rs, beta_override_rs, chi_override_rs FROM eventos"
    ).fetchall()
    con.commit()
    con.close()
    for ev in eventos:
        res = calcular_icseiom(
            ev["data_evento"], ev["lon"], ev["lat"], ev["raio_km"],
            bool(ev["foi_poluente"]), ev["valor_multa_rs"],
            beta_override_rs=ev["beta_override_rs"],
            chi_override_rs=ev["chi_override_rs"],
        )
        con = get_conn(); cur = con.cursor()
        cur.execute(
            "UPDATE resultados SET alpha5_rs=?, icseiom_rs=? WHERE id_evento=?",
            (res.alpha5_rs, res.icseiom_rs, ev["id_evento"]),
        )
        con.commit()
        con.close()


def atualizar_multa_evento(
    id_evento: int, valor_multa_rs: float, provisoria: bool
) -> None:
    """Corrige o valor da multa de um evento poluidor e recalcula resultados."""
    con = get_conn()
    cur = con.cursor()
    ev = cur.execute(
        "SELECT data_evento, lon, lat, raio_km, foi_poluente, "
        "beta_override_rs, chi_override_rs FROM eventos "
        "WHERE id_evento=?", (id_evento,)
    ).fetchone()
    if ev is None:
        con.close()
        raise ValueError(f"evento {id_evento} nao existe")
    if not ev["foi_poluente"]:
        con.close()
        raise ValueError("evento nao-poluidor nao tem multa aplicada")
    cur.execute(
        "UPDATE eventos SET valor_multa_rs=?, multa_provisoria=? WHERE id_evento=?",
        (valor_multa_rs, int(provisoria), id_evento),
    )
    con.commit()
    con.close()
    res = calcular_icseiom(
        ev["data_evento"], ev["lon"], ev["lat"], ev["raio_km"], True,
        valor_multa_rs,
        beta_override_rs=ev["beta_override_rs"],
        chi_override_rs=ev["chi_override_rs"],
    )
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        "UPDATE resultados SET alpha1_rs=?, icseiom_rs=? WHERE id_evento=?",
        (res.alpha1_rs, res.icseiom_rs, id_evento),
    )
    con.commit()
    con.close()
