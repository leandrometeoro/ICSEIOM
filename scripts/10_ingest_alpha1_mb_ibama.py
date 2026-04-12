"""
10_ingest_alpha1_mb_ibama.py

Ingestao REAL do alpha1 (multa ambiental evitada) para o pipeline novo
(baseado em municipios_brasil). Fonte:

  IBAMA / Portal Dados Abertos — Autos de Infracao (SIFISC)
  - main: https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip
  - enq:  https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/enquadramento/enquadramento_csv.zip

Filtro ESTRITO — sô autos relacionados a oleo / hidrocarbonetos /
poluicao marinha. Criterio composto (OR entre as tres vias):

  (1) Enquadramento legal (via tabela enq) em normas especificas de oleo:
      - Lei 9966/2000 (Lei do Oleo)
      - Decreto 4136/2002 (regulamento da Lei do Oleo)
      - Decreto 6514/2008 nos Artigos 61-63 (poluicao) e 66-67 (residuos toxicos)
      - Lei 9605/1998 nos Artigos 54 e 56 (poluicao e substancias toxicas)

  (2) TIPO_INFRACAO == "Poluicao" (case/accent-insensitive).

  (3) Keywords em DES_INFRACAO / DES_AUTO_INFRACAO / FUNDAMENTACAO_MULTA:
      oleo, petroleo, hidrocarboneto, derrame, derramamento, combustivel,
      diesel, efluente oleoso, poluicao marinha.

Agrupa por (COD_MUNICIPIO, ano_do_fato) restrito a municipios costeiros
(municipios_brasil.is_costeiro = 1) e grava em mb_alpha1_multa.

Uso:
    # 1x: baixa e descompacta em /tmp/ibama
    python scripts/10_ingest_alpha1_mb_ibama.py --download

    # depois (reusa o cache):
    python scripts/10_ingest_alpha1_mb_ibama.py
"""
import argparse
import csv
import io
import os
import re
import sqlite3
import sys
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
CACHE = Path("/tmp/ibama")

URLS = {
    "main": "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip",
    "enq":  "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/enquadramento/enquadramento_csv.zip",
}

KEYWORDS = (
    "oleo", "petroleo", "hidrocarboneto", "hidrocarbonetos",
    "derrame", "derramamento", "derramou", "derramar",
    "combustivel", "combustiveis", "diesel", "gasolina",
    "efluente oleoso", "poluicao marinha", "residuo oleoso",
)

# Normas-alvo (match em TP_NORMA + NU_NORMA e, quando relevante, ARTIGO).
# chave: (tp_norma_slug, nu_norma_slug). valor: set de artigos aceitos OU None (todos).
NORMAS_OLEO: dict[tuple[str, str], set[str] | None] = {
    ("lei", "99662000"): None,          # Lei 9966/2000 integral
    ("lei", "9966"): None,
    ("decreto", "41362002"): None,      # Dec 4136/2002 integral
    ("decreto", "4136"): None,
    ("decreto", "65142008"): {"61", "62", "63", "66", "67"},
    ("decreto", "6514"): {"61", "62", "63", "66", "67"},
    ("lei", "96051998"): {"54", "56"},
    ("lei", "9605"): {"54", "56"},
}

FONTE = "IBAMA SIFISC — Autos de Infracao (filtro estrito oleo)"


def p(msg: str) -> None:
    print(msg, flush=True)


def slug(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def parse_valor(s: str) -> float:
    if not s:
        return 0.0
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_ano(row: dict) -> int | None:
    for k in ("DT_FATO_INFRACIONAL", "DAT_HORA_AUTO_INFRACAO", "DT_LANCAMENTO"):
        s = (row.get(k) or "").strip()
        if len(s) >= 4 and s[:4].isdigit():
            ano = int(s[:4])
            if 1990 <= ano <= 2030:
                return ano
    return None


def norma_matches(tp_norma: str, nu_norma: str, artigo: str) -> bool:
    tp = slug(tp_norma)
    nu = slug(nu_norma)
    art = (artigo or "").strip().lstrip("0")
    for (tpk, nuk), arts in NORMAS_OLEO.items():
        if tp == tpk and nu.startswith(nuk):
            if arts is None or art in arts:
                return True
    return False


def download_all():
    CACHE.mkdir(parents=True, exist_ok=True)
    for nome, url in URLS.items():
        target_marker = CACHE / f".{nome}_done"
        if target_marker.exists():
            p(f"[cache] {nome} ja baixado, pulando")
            continue
        p(f"baixando {nome}...")
        r = requests.get(url, timeout=600, stream=True)
        r.raise_for_status()
        data = r.content
        p(f"  {len(data)/1024/1024:.1f} MB")
        z = zipfile.ZipFile(io.BytesIO(data))
        for fn in z.namelist():
            (CACHE / f"{nome}_{fn}").write_bytes(z.read(fn))
        target_marker.write_text("ok")
        p(f"  {len(z.namelist())} arquivos extraidos")


def listar_ceps_costeiros(con: sqlite3.Connection) -> set[str]:
    rows = con.execute(
        "SELECT code_muni FROM municipios_brasil WHERE is_costeiro = 1"
    ).fetchall()
    return {r[0] for r in rows}


def carregar_enq_oleo(anos: list[int]) -> tuple[set[str], dict[str, tuple[str, str, str]]]:
    """Retorna (seqs_oleo, enq_primario).
    seqs_oleo = SEQ_AUTO_INFRACAO com algum enquadramento de oleo.
    enq_primario[seq] = (tp_norma, nu_norma, artigo) - primeiro match por auto.
    """
    seqs: set[str] = set()
    primario: dict[str, tuple[str, str, str]] = {}
    for ano in anos:
        path = CACHE / f"enq_enquadramento_ano_{ano}.csv"
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="replace", newline="") as f:
            rdr = csv.DictReader(f, delimiter=";")
            for row in rdr:
                tp = row.get("TP_NORMA", "") or ""
                nu = row.get("NU_NORMA", "") or ""
                art = row.get("ARTIGO", "") or ""
                if norma_matches(tp, nu, art):
                    seq = (row.get("SEQ_AUTO_INFRACAO") or "").strip()
                    if seq:
                        seqs.add(seq)
                        if seq not in primario:
                            primario[seq] = (tp.strip(), nu.strip(), art.strip())
    return seqs, primario


def keyword_match(row: dict) -> bool:
    bag = " ".join([
        row.get("DES_INFRACAO", "") or "",
        row.get("DES_AUTO_INFRACAO", "") or "",
        row.get("FUNDAMENTACAO_MULTA", "") or "",
    ])
    bag = slug(bag)
    return any(k in bag for k in KEYWORDS)


def tipo_poluicao(row: dict) -> bool:
    return slug(row.get("TIPO_INFRACAO", "")) == "poluicao"


def ingest(anos: list[int], costeiros: set[str]):
    p(f"fase 1/2: carregando enquadramentos de oleo ({len(anos)} anos)...")
    seqs_oleo, enq_primario = carregar_enq_oleo(anos)
    p(f"  {len(seqs_oleo)} autos com enquadramento de oleo")

    p(f"fase 2/2: varrendo auto_infracao...")
    autos: list[dict] = []  # per-auto rows
    agg: dict[tuple[str, int], list[float]] = defaultdict(lambda: [0.0, 0])
    total_linhas = 0
    total_oleo = 0
    total_costeiros = 0

    for ano in anos:
        path = CACHE / f"main_auto_infracao_ano_{ano}.csv"
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="replace", newline="") as f:
            rdr = csv.DictReader(f, delimiter=";")
            for row in rdr:
                total_linhas += 1
                if (row.get("SIT_CANCELADO") or "").strip().upper() == "S":
                    continue
                seq = (row.get("SEQ_AUTO_INFRACAO") or "").strip()

                match_enq = seq in seqs_oleo
                match_tipo = tipo_poluicao(row)
                match_kw = keyword_match(row)
                if not (match_enq or match_tipo or match_kw):
                    continue
                total_oleo += 1

                code = (row.get("COD_MUNICIPIO") or "").strip()
                if len(code) != 7 or not code.isdigit():
                    continue
                if code not in costeiros:
                    continue
                total_costeiros += 1

                ano_fato = parse_ano(row)
                if ano_fato is None:
                    continue
                valor = parse_valor(row.get("VAL_AUTO_INFRACAO", ""))
                if valor <= 0:
                    continue

                # mes do fato para IPCA
                dt = (row.get("DT_FATO_INFRACIONAL") or row.get("DAT_HORA_AUTO_INFRACAO") or "").strip()
                mes_fato = 7
                if len(dt) >= 7 and dt[5:7].isdigit():
                    m = int(dt[5:7])
                    if 1 <= m <= 12:
                        mes_fato = m

                tp_norma = nu_norma = artigo = ""
                if seq in enq_primario:
                    tp_norma, nu_norma, artigo = enq_primario[seq]

                via = "enq" if match_enq else ("tipo" if match_tipo else "kw")

                def g(k: str) -> str:
                    return (row.get(k) or "").strip()

                def gf(k: str) -> float | None:
                    s = g(k).replace(",", ".")
                    if not s:
                        return None
                    try:
                        return float(s)
                    except ValueError:
                        return None

                autos.append({
                    "seq": seq,
                    "code_muni": code,
                    "ano": ano_fato,
                    "mes": mes_fato,
                    "dt_fato": dt[:10] if dt else None,
                    "valor_rs": valor,
                    "tipo_infracao": g("TIPO_INFRACAO"),
                    "des_infracao": g("DES_INFRACAO")[:200],
                    "gravidade": g("GRAVIDADE_INFRACAO") or g("GRAVIDADE") or g("GRAU_INFRACAO"),
                    "cd_nivel_gravidade": g("CD_NIVEL_GRAVIDADE"),
                    "tp_norma": tp_norma,
                    "nu_norma": nu_norma,
                    "artigo": artigo,
                    "match_via": via,
                    "tipo_auto": g("TIPO_AUTO"),
                    "tipo_multa": g("TIPO_MULTA"),
                    "motivacao_conduta": g("MOTIVACAO_CONDUTA"),
                    "efeito_meio_amb": g("EFEITO_MEIO_AMBIENTE"),
                    "efeito_saude": g("EFEITO_SAUDE_PUBLICA"),
                    "passivel_recup": g("PASSIVEL_RECUPERACAO"),
                    "qt_area": gf("QT_AREA"),
                    "infracao_area": g("INFRACAO_AREA"),
                    "classificacao_area": g("CLASSIFICACAO_AREA"),
                    "ds_fator_ajuste": g("DS_FATOR_AJUSTE")[:200],
                    "unid_arrecadacao": g("UNID_ARRECADACAO")[:100],
                    "unid_controle": g("UNID_CONTROLE")[:100],
                    "tp_pessoa_infrator": g("TP_PESSOA_INFRATOR"),
                    "unidade_conservacao": g("UNIDADE_CONSERVACAO")[:200],
                    "ds_biomas": g("DS_BIOMAS_ATINGIDOS")[:200],
                    "tipo_acao": g("TIPO_ACAO"),
                    "operacao": g("OPERACAO")[:100],
                    "tp_origem_registro": g("TP_ORIGEM_REGISTRO_AUTO"),
                })

                bucket = agg[(code, ano_fato)]
                bucket[0] += valor
                bucket[1] += 1
        p(f"  [{ano}] linhas acumuladas: {total_linhas}  oleo: {total_oleo}  costeiros: {total_costeiros}")

    p("")
    p(f"resumo: {total_linhas} autos lidos, {total_oleo} matcharam oleo, {total_costeiros} em munis costeiros")
    p(f"autos individuais gravaveis: {len(autos)}")
    p(f"chaves (code_muni, ano) agregadas: {len(agg)}")
    return autos, agg


def gravar(autos: list[dict], agg: dict[tuple[str, int], list[float]]):
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")

    # ---- per-auto (nova tabela) ----
    con.execute("DROP TABLE IF EXISTS mb_alpha1_autos")
    con.execute("""
        CREATE TABLE mb_alpha1_autos (
            seq_auto            TEXT PRIMARY KEY,
            code_muni           TEXT NOT NULL,
            ano                 INTEGER NOT NULL,
            mes                 INTEGER NOT NULL,
            dt_fato             TEXT,
            valor_rs            REAL NOT NULL,
            tipo_infracao       TEXT,
            des_infracao        TEXT,
            gravidade           TEXT,
            cd_nivel_gravidade  TEXT,
            tp_norma            TEXT,
            nu_norma            TEXT,
            artigo              TEXT,
            match_via           TEXT,
            tipo_auto           TEXT,
            tipo_multa          TEXT,
            motivacao_conduta   TEXT,
            efeito_meio_amb     TEXT,
            efeito_saude        TEXT,
            passivel_recup      TEXT,
            qt_area             REAL,
            infracao_area       TEXT,
            classificacao_area  TEXT,
            ds_fator_ajuste     TEXT,
            unid_arrecadacao    TEXT,
            unid_controle       TEXT,
            tp_pessoa_infrator  TEXT,
            unidade_conservacao TEXT,
            ds_biomas           TEXT,
            tipo_acao           TEXT,
            operacao            TEXT,
            tp_origem_registro  TEXT
        )
    """)
    con.execute("CREATE INDEX idx_mb_a1a_muni ON mb_alpha1_autos(code_muni)")
    con.execute("CREATE INDEX idx_mb_a1a_ano ON mb_alpha1_autos(ano)")
    con.execute("CREATE INDEX idx_mb_a1a_tipo ON mb_alpha1_autos(tipo_infracao)")

    # de-dup por seq (alguns autos podem aparecer em mais de 1 ano csv)
    vistos = set()
    rows_auto = []
    for a in autos:
        if not a["seq"] or a["seq"] in vistos:
            continue
        vistos.add(a["seq"])
        rows_auto.append((
            a["seq"], a["code_muni"], a["ano"], a["mes"], a["dt_fato"],
            round(a["valor_rs"], 2), a["tipo_infracao"], a["des_infracao"],
            a["gravidade"], a["cd_nivel_gravidade"],
            a["tp_norma"], a["nu_norma"], a["artigo"], a["match_via"],
            a["tipo_auto"], a["tipo_multa"],
            a["motivacao_conduta"], a["efeito_meio_amb"], a["efeito_saude"],
            a["passivel_recup"], a["qt_area"],
            a["infracao_area"], a["classificacao_area"], a["ds_fator_ajuste"],
            a["unid_arrecadacao"], a["unid_controle"], a["tp_pessoa_infrator"],
            a["unidade_conservacao"], a["ds_biomas"],
            a["tipo_acao"], a["operacao"], a["tp_origem_registro"],
        ))
    con.executemany(
        "INSERT INTO mb_alpha1_autos VALUES (" + ",".join(["?"] * 32) + ")",
        rows_auto,
    )
    p(f"[OK] mb_alpha1_autos: {len(rows_auto)} autos individuais")

    # ---- agregado (mantido para compat) ----
    con.execute("DELETE FROM mb_alpha1_multa")
    rows_agg = [
        (code, ano, round(v[0], 2), int(v[1]), FONTE)
        for (code, ano), v in agg.items()
    ]
    con.executemany(
        "INSERT INTO mb_alpha1_multa (code_muni, ano, valor_rs, n_autos, fonte) "
        "VALUES (?, ?, ?, ?, ?)",
        rows_agg,
    )
    anos = sorted({ano for _, ano in agg.keys()})
    safra = f"{min(anos)}-{max(anos)}" if anos else "—"
    con.execute(
        "INSERT INTO metadados_atualizacao (fonte, ultima_safra, atualizado_em, url, observacoes) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT(fonte) DO UPDATE SET "
        "ultima_safra=excluded.ultima_safra, atualizado_em=excluded.atualizado_em, "
        "url=excluded.url, observacoes=excluded.observacoes",
        (
            "mb_alpha1_multa",
            safra,
            datetime.utcnow().isoformat(timespec="seconds"),
            URLS["main"],
            FONTE,
        ),
    )
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM mb_alpha1_multa").fetchone()[0]
    s = con.execute("SELECT COALESCE(SUM(valor_rs),0) FROM mb_alpha1_multa").fetchone()[0]
    con.close()
    p(f"[OK] mb_alpha1_multa: {n} linhas, total = R$ {s:,.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true", help="baixar os zips do IBAMA (se ja baixou, pula)")
    ap.add_argument("--desde", type=int, default=2005, help="ano inicial (default 2005)")
    ap.add_argument("--ate", type=int, default=2026, help="ano final inclusivo (default 2026)")
    args = ap.parse_args()

    if args.download:
        download_all()

    if not any(CACHE.glob("main_auto_infracao_ano_*.csv")):
        p("ERRO: cache vazio. Rode com --download primeiro.")
        sys.exit(1)

    anos = list(range(args.desde, args.ate + 1))
    con = sqlite3.connect(DB)
    costeiros = listar_ceps_costeiros(con)
    con.close()
    p(f"municipios costeiros no DB: {len(costeiros)}")

    autos, agg = ingest(anos, costeiros)
    gravar(autos, agg)


if __name__ == "__main__":
    main()
