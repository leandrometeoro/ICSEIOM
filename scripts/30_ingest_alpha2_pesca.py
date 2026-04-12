"""
30_ingest_alpha2_pesca.py

Ingere dados de aquicultura do IBGE SIDRA (tabela 3940, PPM) para α₂.

Fonte
-----
IBGE SIDRA tabela 3940 — Produção da aquicultura, por tipo de produto.
  - var 215:  valor da produção (Mil Reais)
  - var 4146: quantidade produzida (toneladas, unidade varia por produto)
  - classificação 654, categoria 0 = Total
  - nível N6 = município
  - períodos: 2013-2024

Nota: a tabela 3940 cobre aquicultura (cultivo), não pesca extrativa
marinha. Não existe tabela SIDRA com pesca extrativa por município
(série encerrada em 2009, só até UF). Usamos aquicultura como proxy
até que dados do BEPA/MPA estejam disponíveis em formato estruturado.
O script aceita também carga manual de planilha MPA (ver --csv).

Pipeline
--------
1) Lê os 443 code_muni costeiros de mb_features_muni.
2) Baixa var 215 e 4146 da API SIDRA em lotes de 50 municípios.
3) Converte "Mil Reais" para Reais (×1000).
4) Insere em alpha2_pesca (code_muni, ano, valor_rs, toneladas, fonte).
5) Registra em metadados_atualizacao.

Uso
---
  python scripts/30_ingest_alpha2_pesca.py           # API SIDRA
  python scripts/30_ingest_alpha2_pesca.py --csv X    # carga manual
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sqlite3
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

SIDRA_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/3940"
PERIODOS = "2013-2024"
BATCH = 50
FONTE = "IBGE SIDRA 3940 (aquicultura PPM)"

# Produtos com unidade em quilogramas (var 4146) — podem ser somados
# 79366=Peixes (subtotal), 32887=Camarão, 32889=Ostras/vieiras/mexilhões
CATS_KG = "79366,32887,32889"


def p(msg: str) -> None:
    print(msg, flush=True)


def fetch_json(url: str, tentativas: int = 3) -> list | dict:
    for t in range(tentativas):
        try:
            req = urllib.request.Request(url, headers={
                "Accept-Encoding": "gzip",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                return json.loads(raw)
        except Exception as e:
            if t == tentativas - 1:
                raise
            p(f"  tentativa {t+1} falhou ({e}), retentando...")
            time.sleep(2 * (t + 1))
    return []


def baixar_sidra(codes: list[str]) -> dict[tuple[str, int], dict]:
    """Retorna {(code_muni, ano): {valor_mil_rs, kg}} para os códigos.

    Valor: var 215, classificacao 654[0] (total, Mil Reais).
    Quantidade: var 4146, classificacao 654[peixes,camarao,ostras] em kg,
    somados por municipio×ano. Produtos em outras unidades (milheiros,
    nenhuma) sao ignorados pois nao convertem para kg.
    """
    dados: dict[tuple[str, int], dict] = {}

    for i in range(0, len(codes), BATCH):
        chunk = codes[i:i + BATCH]
        codelist = ",".join(chunk)
        p(f"  lote {i // BATCH + 1}: {len(chunk)} munis...")

        # var 215 = valor da produção (Mil Reais), total
        url_val = (
            f"{SIDRA_BASE}/periodos/{PERIODOS}/variaveis/215"
            f"?localidades=N6[{codelist}]&classificacao=654[0]"
        )
        resp_val = fetch_json(url_val)
        if resp_val and isinstance(resp_val, list):
            for serie in resp_val[0].get("resultados", [{}])[0].get("series", []):
                code = serie["localidade"]["id"]
                for ano_str, val_str in serie.get("serie", {}).items():
                    ano = int(ano_str)
                    if val_str and val_str not in ("...", "..", "-", "X", ""):
                        dados.setdefault((code, ano), {})["valor_mil_rs"] = float(val_str)

        time.sleep(0.5)

        # var 4146 = quantidade produzida (kg) por produto
        url_qty = (
            f"{SIDRA_BASE}/periodos/{PERIODOS}/variaveis/4146"
            f"?localidades=N6[{codelist}]&classificacao=654[{CATS_KG}]"
        )
        resp_qty = fetch_json(url_qty)
        if resp_qty and isinstance(resp_qty, list):
            for res in resp_qty[0].get("resultados", []):
                for serie in res.get("series", []):
                    code = serie["localidade"]["id"]
                    for ano_str, val_str in serie.get("serie", {}).items():
                        ano = int(ano_str)
                        if val_str and val_str not in ("...", "..", "-", "X", ""):
                            dados.setdefault((code, ano), {}).setdefault("kg", 0.0)
                            dados[(code, ano)]["kg"] += float(val_str)

        time.sleep(0.5)

    return dados


def ingest_sidra() -> None:
    con = sqlite3.connect(DB)
    cur = con.cursor()

    codes = [r[0] for r in cur.execute(
        "SELECT code_muni FROM mb_features_muni ORDER BY code_muni"
    ).fetchall()]
    p(f"municipios costeiros: {len(codes)}")

    p("baixando SIDRA 3940...")
    dados = baixar_sidra(codes)
    p(f"celulas com dado: {len(dados)}")

    cur.execute("DELETE FROM alpha2_pesca")
    n = 0
    n_com_ton = 0
    for (code, ano), d in sorted(dados.items()):
        valor_mil = d.get("valor_mil_rs")
        if valor_mil is None:
            continue
        valor_rs = valor_mil * 1000.0
        kg = d.get("kg")
        toneladas = kg / 1000.0 if kg else None
        if toneladas:
            n_com_ton += 1
        cur.execute(
            "INSERT OR REPLACE INTO alpha2_pesca "
            "(code_muni, ano, valor_rs, toneladas, fonte) VALUES (?,?,?,?,?)",
            (code, ano, valor_rs, toneladas, FONTE),
        )
        n += 1
    p(f"inseridas {n} celulas, {n_com_ton} com toneladas ({n - n_com_ton} sem)")

    # metadados
    from datetime import datetime
    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao "
        "(fonte, ultima_safra, atualizado_em, url, observacoes) "
        "VALUES (?,?,?,?,?)",
        (
            "alpha2_pesca",
            PERIODOS,
            datetime.now().isoformat(timespec="seconds"),
            "https://sidra.ibge.gov.br/tabela/3940",
            "Aquicultura PPM (proxy para pesca). "
            "Pesca extrativa marinha por municipio nao disponivel no SIDRA. "
            "Quando dados BEPA/MPA estiverem em planilha, usar --csv.",
        ),
    )
    con.commit()

    # resumo
    resumo = cur.execute(
        "SELECT COUNT(DISTINCT code_muni), COUNT(*), "
        "ROUND(SUM(valor_rs), 0), MIN(ano), MAX(ano) "
        "FROM alpha2_pesca"
    ).fetchone()
    p(f"\nalpha2_pesca: {resumo[0]} municipios, {resumo[1]} linhas, "
      f"R$ {resumo[2]:,.0f} total, {resumo[3]}-{resumo[4]}")

    # top 5
    top = cur.execute(
        "SELECT a.code_muni, m.nome, m.uf, ROUND(SUM(a.valor_rs),0) t "
        "FROM alpha2_pesca a "
        "LEFT JOIN municipios_costeiros m ON m.code_muni = a.code_muni "
        "GROUP BY a.code_muni ORDER BY t DESC LIMIT 5"
    ).fetchall()
    p("top 5 municipios (soma 2013-2024):")
    for r in top:
        nome = r[1] or "?"
        p(f"  {r[0]} {nome}-{r[2] or '?'}: R$ {r[3]:,.0f}")

    con.close()
    p("ok")


def ingest_csv(path: str) -> None:
    """Carga manual a partir de CSV/planilha exportada.

    Espera colunas: code_muni, ano, valor_rs, toneladas (opcional).
    Separador: , ou ;. Encoding: utf-8 ou latin-1.
    """
    p(f"lendo {path}...")
    rows = []
    for enc in ("utf-8", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                sample = f.read(2048)
                sep = ";" if ";" in sample else ","
                f.seek(0)
                reader = csv.DictReader(f, delimiter=sep)
                for r in reader:
                    code = r.get("code_muni", "").strip()
                    ano = r.get("ano", "").strip()
                    val = r.get("valor_rs", "").strip().replace(",", ".")
                    ton = r.get("toneladas", "").strip().replace(",", ".")
                    if code and ano and val:
                        rows.append((
                            code, int(ano), float(val),
                            float(ton) if ton else None,
                            "MPA/BEPA (carga manual CSV)",
                        ))
            break
        except UnicodeDecodeError:
            continue

    if not rows:
        p("nenhuma linha valida encontrada")
        return

    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("DELETE FROM alpha2_pesca")
    cur.executemany(
        "INSERT OR REPLACE INTO alpha2_pesca "
        "(code_muni, ano, valor_rs, toneladas, fonte) VALUES (?,?,?,?,?)",
        rows,
    )
    con.commit()
    p(f"inseridas {len(rows)} linhas")
    con.close()
    p("ok")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", help="caminho para CSV/planilha manual (MPA/BEPA)")
    args = parser.parse_args()

    if args.csv:
        ingest_csv(args.csv)
    else:
        ingest_sidra()


if __name__ == "__main__":
    main()
