"""
31_ingest_alpha2_bepa.py

Ingere dados de pesca extrativa marinha do BEPA/MPA (Boletim da Reconstrução
da Pesca Marinha 1950-2022) e distribui para municípios costeiros usando
pesos diferenciados por setor:
  - Artesanal → proporcional ao nº de pescadores (SisRGP Pescador)
  - Industrial → proporcional ao nº de armadores (SisRGP Armador)

Fontes
------
1. MPA BEPA "Banco_reconstrucao_marinha_*.csv"
   - Captura em toneladas por Estado/Ano/Família/Grande grupo/Setor
   - 116 mil linhas, 17 estados costeiros, 1950-2022

2. MPA SisRGP "Pescador/{UF} Pescadores(a).csv"
   - Pescadores registrados por município (Código IBGE)
   - Peso para distribuição da produção artesanal

3. MPA SisRGP "Armador(Sheet1).csv"
   - Armadores de pesca por município (nome da cidade + UF)
   - Peso para distribuição da produção industrial

4. Preços por categoria (R$/ton), armazenados em tabela parametros.

Uso
---
  python scripts/31_ingest_alpha2_bepa.py --bepa CSV --rgp DIR --armador CSV
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

CATEGORIAS = ["peixes", "camaroes", "lagostas", "siris_carang", "moluscos", "outros"]

PRECOS_DEFAULT: dict[str, float] = {
    "peixes": 8_000.0,
    "camaroes": 25_000.0,
    "lagostas": 80_000.0,
    "siris_carang": 15_000.0,
    "moluscos": 15_000.0,
    "outros": 10_000.0,
}

FAMILIA_CAMARAO = {
    "Penaeidae", "Solenoceridae", "Pandalidae", "Sergestidae",
}
FAMILIA_LAGOSTA = {
    "Palinuridae", "Scyllaridae", "Nephropidae",
}
FAMILIA_SIRI_CARANG = {
    "Portunidae", "Ocypodidae", "Grapsidae", "Gecarcinidae",
    "Carpiliidae", "Menippidae", "Mithracidae", "Callichiridae",
    "Geryonidae",
}

UF_SIGLA = {
    "Alagoas": "AL", "Amapá": "AP", "Bahia": "BA", "Ceará": "CE",
    "Espírito Santo": "ES", "Maranhão": "MA", "Pará": "PA",
    "Paraíba": "PB", "Pernambuco": "PE", "Piauí": "PI",
    "Paraná": "PR", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Santa Catarina": "SC", "Sergipe": "SE",
    "São Paulo": "SP",
}


def p(msg: str) -> None:
    print(msg, flush=True)


def classificar(grande_grupo: str, familia: str) -> str:
    if grande_grupo == "Moluscos":
        return "moluscos"
    if grande_grupo == "Crustáceos":
        if familia in FAMILIA_CAMARAO:
            return "camaroes"
        if familia in FAMILIA_LAGOSTA:
            return "lagostas"
        if familia in FAMILIA_SIRI_CARANG:
            return "siris_carang"
        return "camaroes"
    if grande_grupo == "Peixes":
        return "peixes"
    return "outros"


def ler_bepa(path: str) -> dict[tuple[str, int, str, str], float]:
    """Agrega captura (ton) por (UF_sigla, ano, categoria, setor) do CSV BEPA."""
    totais: dict[tuple[str, int, str, str], float] = defaultdict(float)
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                reader = csv.DictReader(f, delimiter=";")
                cols = reader.fieldnames or []
                col_familia = next(
                    (c for c in cols if "fam" in c.lower() and "lia" in c.lower()), "Família"
                )
                col_grupo = next(
                    (c for c in cols if "grande" in c.lower()), "Grande grupo"
                )
                n = 0
                for row in reader:
                    estado = row.get("Estado", "").strip()
                    ano_str = row.get("Ano", "").strip()
                    cap_str = row.get("Captura (t)", "").strip().replace(",", ".")
                    grande_grupo = row.get(col_grupo, "").strip()
                    familia = row.get(col_familia, "").strip()
                    setor = row.get("Setor", "").strip()
                    uf = UF_SIGLA.get(estado)
                    if not uf or not ano_str or not cap_str:
                        continue
                    if setor not in ("Artesanal", "Industrial"):
                        continue
                    cat = classificar(grande_grupo, familia)
                    try:
                        totais[(uf, int(ano_str), cat, setor)] += float(cap_str)
                        n += 1
                    except ValueError:
                        continue
            p(f"BEPA: {n} linhas lidas, {len(totais)} celulas (UF, ano, cat, setor)")
            art = sum(v for (_, _, _, s), v in totais.items() if s == "Artesanal")
            ind = sum(v for (_, _, _, s), v in totais.items() if s == "Industrial")
            p(f"  Artesanal: {art:,.0f} ton, Industrial: {ind:,.0f} ton")
            return dict(totais)
        except UnicodeDecodeError:
            continue
    p("erro: nao conseguiu ler BEPA CSV")
    return {}


def ler_rgp(rgp_dir: str) -> dict[str, int]:
    """Conta pescadores por code_muni a partir dos CSVs SisRGP Pescador."""
    pescadores: dict[str, int] = defaultdict(int)
    d = Path(rgp_dir)
    arquivos = sorted(d.glob("*.csv"))
    if not arquivos:
        p(f"nenhum CSV encontrado em {rgp_dir}")
        return {}

    total = 0
    for arq in arquivos:
        for enc in ("utf-8-sig", "latin-1"):
            try:
                with open(arq, encoding=enc) as f:
                    reader = csv.DictReader(f, delimiter=";")
                    for row in reader:
                        code = row.get("Código IBGE", "").strip()
                        if code and len(code) == 7:
                            pescadores[code] += 1
                            total += 1
                break
            except UnicodeDecodeError:
                continue

    p(f"SisRGP Pescador: {total:,} pescadores em {len(pescadores)} municipios ({len(arquivos)} arquivos)")
    return dict(pescadores)


def ler_armador(path: str, muni_nome_to_code: dict[tuple[str, str], str]) -> dict[str, int]:
    """Conta armadores por code_muni a partir do CSV SisRGP Armador.

    O CSV tem colunas: ;Nome;Vínculo;Número;cidade;UF;Tipo;Índice
    cidade vem como "Belém,PA,Brasil" ou "Florianópolis,SC,Brasil".
    """
    armadores: dict[str, int] = defaultdict(int)
    total = 0
    sem_match = 0

    for enc in ("utf-8-sig", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    cidade_raw = row.get("cidade", "").strip()
                    if not cidade_raw:
                        continue
                    partes = [p.strip() for p in cidade_raw.split(",")]
                    if len(partes) < 2:
                        continue
                    nome_muni = partes[0]
                    uf = partes[1]
                    key = (normalizar(nome_muni), uf)
                    code = muni_nome_to_code.get(key)
                    if code:
                        armadores[code] += 1
                        total += 1
                    else:
                        sem_match += 1
            p(f"SisRGP Armador: {total:,} armadores em {len(armadores)} municipios")
            if sem_match:
                p(f"  ({sem_match} registros sem match de municipio)")
            return dict(armadores)
        except UnicodeDecodeError:
            continue

    p("erro: nao conseguiu ler Armador CSV")
    return {}


def normalizar(nome: str) -> str:
    """Normaliza nome de município para matching."""
    import unicodedata
    s = unicodedata.normalize("NFKD", nome)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def construir_muni_nome_to_code(cur: sqlite3.Cursor) -> dict[tuple[str, str], str]:
    """Mapeia (nome_normalizado, UF) → code_muni usando municipios_brasil."""
    mapa: dict[tuple[str, str], str] = {}
    for code, nome, uf in cur.execute(
        "SELECT code_muni, nome, uf FROM municipios_brasil"
    ).fetchall():
        mapa[(normalizar(nome), uf)] = code
    return mapa


def distribuir(
    bepa: dict[tuple[str, int, str, str], float],
    uf_munis: dict[str, list[str]],
    pescadores: dict[str, int],
    armadores: dict[str, int],
    precos: dict[str, float],
    ano_min: int,
    ano_max: int,
) -> dict[tuple[str, int], dict]:
    """Distribui toneladas BEPA por município usando peso diferenciado por setor."""
    registros: dict[tuple[str, int], dict] = defaultdict(
        lambda: {"toneladas": 0.0, "valor_rs": 0.0, "fontes": set()}
    )
    stats = {"art_ok": 0, "ind_ok": 0, "art_uniforme": 0, "ind_uniforme": 0}

    for (uf, ano, cat, setor), ton_uf in sorted(bepa.items()):
        if ano < ano_min or ano > ano_max:
            continue

        munis = uf_munis.get(uf, [])
        if not munis:
            continue

        preco_ton = precos[cat]

        if setor == "Artesanal":
            pesos = {c: pescadores.get(c, 0) for c in munis}
        else:
            pesos = {c: armadores.get(c, 0) for c in munis}

        total_peso = sum(pesos.values())

        if total_peso == 0:
            n_munis = len(munis)
            stat_key = "art_uniforme" if setor == "Artesanal" else "ind_uniforme"
            stats[stat_key] += 1
            for code in munis:
                ton_muni = ton_uf / n_munis
                r = registros[(code, ano)]
                r["toneladas"] += ton_muni
                r["valor_rs"] += ton_muni * preco_ton
                r["fontes"].add(f"BEPA-{setor[:3]} (uniforme)")
            continue

        stat_key = "art_ok" if setor == "Artesanal" else "ind_ok"
        stats[stat_key] += 1
        for code in munis:
            w = pesos.get(code, 0)
            if w == 0:
                continue
            peso = w / total_peso
            ton_muni = ton_uf * peso
            r = registros[(code, ano)]
            r["toneladas"] += ton_muni
            r["valor_rs"] += ton_muni * preco_ton
            if setor == "Artesanal":
                r["fontes"].add("BEPA-Art+RGP")
            else:
                r["fontes"].add("BEPA-Ind+Armador")

    p(f"distribuicao:")
    p(f"  Artesanal: {stats['art_ok']} celulas com peso RGP, {stats['art_uniforme']} uniforme")
    p(f"  Industrial: {stats['ind_ok']} celulas com peso Armador, {stats['ind_uniforme']} uniforme")
    p(f"celulas pesca extrativa: {len(registros)}")
    return registros


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bepa", required=True, help="CSV BEPA reconstrução pesca marinha")
    parser.add_argument("--rgp", required=True, help="diretório com CSVs SisRGP Pescador/")
    parser.add_argument("--armador", required=True, help="CSV SisRGP Armador")
    parser.add_argument("--preco-peixes", type=float, default=PRECOS_DEFAULT["peixes"])
    parser.add_argument("--preco-camaroes", type=float, default=PRECOS_DEFAULT["camaroes"])
    parser.add_argument("--preco-lagostas", type=float, default=PRECOS_DEFAULT["lagostas"])
    parser.add_argument("--preco-siris-carang", type=float, default=PRECOS_DEFAULT["siris_carang"])
    parser.add_argument("--preco-moluscos", type=float, default=PRECOS_DEFAULT["moluscos"])
    parser.add_argument("--preco-outros", type=float, default=PRECOS_DEFAULT["outros"])
    parser.add_argument("--ano-min", type=int, default=2013, help="ano mínimo (default 2013)")
    parser.add_argument("--ano-max", type=int, default=2024, help="ano máximo (default 2024)")
    args = parser.parse_args()

    precos: dict[str, float] = {
        "peixes": args.preco_peixes,
        "camaroes": args.preco_camaroes,
        "lagostas": args.preco_lagostas,
        "siris_carang": args.preco_siris_carang,
        "moluscos": args.preco_moluscos,
        "outros": args.preco_outros,
    }

    p("=== Ingestão α₂ pesca extrativa marinha (BEPA + SisRGP + Armador) ===")
    p("precos R$/ton:")
    for cat, pr in precos.items():
        p(f"  {cat:15s} R$ {pr:>10,.0f}")

    bepa = ler_bepa(args.bepa)
    if not bepa:
        return

    pescadores = ler_rgp(args.rgp)
    if not pescadores:
        return

    con = sqlite3.connect(DB)
    cur = con.cursor()

    muni_nome_to_code = construir_muni_nome_to_code(cur)
    armadores = ler_armador(args.armador, muni_nome_to_code)
    if not armadores:
        p("AVISO: sem armadores, industrial será distribuído uniformemente")

    costeiros = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT f.code_muni, b.uf "
            "FROM mb_features_muni f "
            "JOIN municipios_brasil b ON b.code_muni = f.code_muni"
        ).fetchall()
    }
    p(f"\nmunicipios costeiros: {len(costeiros)}")

    uf_munis: dict[str, list[str]] = defaultdict(list)
    for code, uf in costeiros.items():
        uf_munis[uf].append(code)

    registros = distribuir(bepa, uf_munis, pescadores, armadores, precos,
                           args.ano_min, args.ano_max)

    # ler aquicultura existente (SIDRA 3940) antes de apagar
    aqui: dict[tuple[str, int], dict] = {}
    for r in cur.execute(
        "SELECT code_muni, ano, valor_rs, toneladas FROM alpha2_pesca"
    ).fetchall():
        aqui[(r[0], r[1])] = {"valor_rs": r[2] or 0.0, "toneladas": r[3]}
    p(f"celulas aquicultura existente (SIDRA 3940): {len(aqui)}")

    # combinar: α₂ = pesca extrativa + aquicultura
    todos_keys = set(registros.keys()) | set(aqui.keys())
    p(f"celulas combinadas: {len(todos_keys)}")

    cur.execute("DELETE FROM alpha2_pesca")
    n = 0
    for key in sorted(todos_keys):
        code, ano = key
        ext = registros.get(key)
        aqu = aqui.get(key)

        valor_ext = ext["valor_rs"] if ext else 0.0
        valor_aqu = aqu["valor_rs"] if aqu else 0.0
        ton_ext = ext["toneladas"] if ext else 0.0
        ton_aqu = (aqu["toneladas"] or 0.0) if aqu else 0.0

        valor_total = valor_ext + valor_aqu
        ton_total = ton_ext + ton_aqu if (ton_ext or ton_aqu) else None

        fontes = []
        if ext and valor_ext > 0:
            fontes.extend(sorted(ext["fontes"]))
        if aqu and valor_aqu > 0:
            fontes.append("SIDRA 3940")
        fonte = " + ".join(fontes) if fontes else "sem dado"

        if valor_total > 0:
            cur.execute(
                "INSERT OR REPLACE INTO alpha2_pesca "
                "(code_muni, ano, valor_rs, toneladas, fonte) VALUES (?,?,?,?,?)",
                (code, ano, valor_total, ton_total, fonte),
            )
            n += 1

    # salvar precos como parametros
    PARAM_DESC = {
        "peixes": "Preço médio primeira venda peixes marinhos (sardinha, corvina, tainha, atum etc.)",
        "camaroes": "Preço médio primeira venda camarões marinhos (Penaeidae, Solenoceridae)",
        "lagostas": "Preço médio primeira venda lagostas (Palinuridae, Scyllaridae)",
        "siris_carang": "Preço médio primeira venda siris e caranguejos (Portunidae, Ocypodidae etc.)",
        "moluscos": "Preço médio primeira venda moluscos marinhos (lula, polvo, marisco, ostra etc.)",
        "outros": "Preço médio primeira venda outros invertebrados marinhos",
    }
    for cat in CATEGORIAS:
        cur.execute(
            "INSERT OR REPLACE INTO parametros (chave, valor, unidade, descricao) "
            "VALUES (?,?,?,?)",
            (f"preco_ton_{cat}", str(precos[cat]), "R$/ton",
             f"{PARAM_DESC[cat]}. Ref: CONAB/CEAGESP. Ajustável."),
        )

    # metadados
    precos_str = ", ".join(f"{c}: R$ {precos[c]:,.0f}" for c in CATEGORIAS)
    cur.execute(
        "INSERT OR REPLACE INTO metadados_atualizacao "
        "(fonte, ultima_safra, atualizado_em, url, observacoes) VALUES (?,?,?,?,?)",
        (
            "alpha2_pesca",
            f"{args.ano_min}-{args.ano_max}",
            datetime.now().isoformat(timespec="seconds"),
            "https://www.gov.br/mpa/pt-br/assuntos/pesca/bepa",
            f"Pesca extrativa: BEPA reconstrução 1950-2022 (ton por UF×família×setor). "
            f"Artesanal distribuída por peso SisRGP Pescador (n_pescadores), "
            f"Industrial distribuída por peso SisRGP Armador (n_armadores). "
            f"Preços/ton: {precos_str}. "
            f"Aquicultura: SIDRA 3940 (valor R$).",
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

    # top 10
    top = cur.execute(
        "SELECT a.code_muni, b.nome, ROUND(SUM(a.valor_rs),0) t "
        "FROM alpha2_pesca a "
        "LEFT JOIN municipios_brasil b ON b.code_muni = a.code_muni "
        "GROUP BY a.code_muni ORDER BY t DESC LIMIT 10"
    ).fetchall()
    p("top 10 municipios (soma total):")
    for r in top:
        nome = r[1] or "?"
        p(f"  {r[0]} {nome}: R$ {r[2]:,.0f}")

    # composicao
    ext_total = sum(r["valor_rs"] for r in registros.values())
    aqu_total = sum(r["valor_rs"] for r in aqui.values())
    p(f"\ncomposicao: extrativa R$ {ext_total:,.0f} + aquicultura R$ {aqu_total:,.0f}")

    con.close()
    p("ok")


if __name__ == "__main__":
    main()
