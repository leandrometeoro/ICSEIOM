"""
registrar_evento.py — CLI para registrar um incidente e calcular o ICSEIOM.

Uso:
    python registrar_evento.py --data 2026-04-05 --lat -22.95 --lon -42.02 \
        --raio_km 15 --foi_poluente false --descricao "Mancha ao largo de Arraial do Cabo"
"""
import argparse, sqlite3, json
from pathlib import Path
from calcular_icseiom import calcular_icseiom, imprimir

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, help="YYYY-MM-DD")
    p.add_argument("--lon",  required=True, type=float)
    p.add_argument("--lat",  required=True, type=float)
    p.add_argument("--raio_km", required=True, type=float)
    p.add_argument("--foi_poluente", default="true", choices=["true","false"])
    p.add_argument("--descricao", default="")
    a = p.parse_args()

    foi = a.foi_poluente == "true"

    res = calcular_icseiom(a.data, a.lon, a.lat, a.raio_km, foi)
    imprimir(res)

    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute(
        """INSERT INTO eventos (data_evento, lon, lat, raio_km, foi_poluente, descricao)
           VALUES (?,?,?,?,?,?)""",
        (a.data, a.lon, a.lat, a.raio_km, 1 if foi else 0, a.descricao),
    )
    id_ev = cur.lastrowid
    for m in res.municipios:
        cur.execute(
            "INSERT INTO eventos_municipios (id_evento, code_muni, fracao) VALUES (?,?,?)",
            (id_ev, m["code"], m["frac"]),
        )
    cur.execute(
        """INSERT INTO resultados
           (id_evento, alpha1_rs, alpha2_rs, alpha3_rs, alpha4_rs, alpha5_rs,
            beta_rs, chi_rs, k_aplicado, icseiom_rs)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (id_ev, res.alpha1_rs, res.alpha2_rs, res.alpha3_rs, res.alpha4_rs,
         res.alpha5_rs, res.beta_rs, res.chi_rs, res.k, res.icseiom_rs),
    )
    con.commit()
    con.close()
    print(f"\n[OK] Evento {id_ev} registrado no banco.")

if __name__ == "__main__":
    main()
