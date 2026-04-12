"""
00_init_db.py
Cria o schema do banco ICSEIOM em SQLite puro (sem SpatiaLite obrigatório).
Geometrias são armazenadas como WKT — GeoPandas le via shapely.wkt.
"""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "db" / "icseiom.db"
DB.parent.mkdir(parents=True, exist_ok=True)

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS municipios_brasil (
    code_muni   TEXT PRIMARY KEY,       -- codigo IBGE 7 digitos
    nome        TEXT NOT NULL,
    uf          TEXT NOT NULL,
    regiao      TEXT NOT NULL,
    is_costeiro INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_mb_uf ON municipios_brasil(uf);
CREATE INDEX IF NOT EXISTS idx_mb_costeiro ON municipios_brasil(is_costeiro);

-- Pipeline paralelo (novo mapa de municipios): tabelas mb_alphaN
-- Nao tem FK pra evitar que ingestoes quebrem em munis fora dos 443 costeiros;
-- a selecao eh feita via JOIN com municipios_brasil.is_costeiro no momento da query.
CREATE TABLE IF NOT EXISTS mb_alpha1_multa (
    code_muni   TEXT NOT NULL,           -- codigo IBGE 7 digitos
    ano         INTEGER NOT NULL,
    valor_rs    REAL NOT NULL,           -- soma dos valores em R$
    n_autos     INTEGER,                 -- numero de autos de infracao
    fonte       TEXT,
    PRIMARY KEY (code_muni, ano)
);
CREATE INDEX IF NOT EXISTS idx_mb_a1_ano ON mb_alpha1_multa(ano);

CREATE TABLE IF NOT EXISTS municipios_costeiros (
    code_muni   TEXT PRIMARY KEY,       -- codigo IBGE 7 digitos
    nome        TEXT NOT NULL,
    uf          TEXT NOT NULL,
    regiao      TEXT NOT NULL,
    area_km2    REAL,
    pop_2022    INTEGER,
    lon_centro  REAL,
    lat_centro  REAL,
    geom_wkt    TEXT                    -- POLYGON ((...))
);

CREATE TABLE IF NOT EXISTS alpha1_multa_ambiental (
    code_muni   TEXT NOT NULL,
    ano         INTEGER NOT NULL,
    valor_rs    REAL NOT NULL,          -- R$ anual
    n_autos     INTEGER,
    fonte       TEXT,
    PRIMARY KEY (code_muni, ano),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS alpha2_pesca (
    code_muni   TEXT NOT NULL,
    ano         INTEGER NOT NULL,
    valor_rs    REAL NOT NULL,          -- R$ bruto anual
    toneladas   REAL,
    fonte       TEXT,
    PRIMARY KEY (code_muni, ano),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS alpha3_turismo (
    code_muni   TEXT NOT NULL,
    ano         INTEGER NOT NULL,
    vab_aloj_rs REAL NOT NULL,          -- R$ VAB alojamento/alimentacao
    fonte       TEXT,
    PRIMARY KEY (code_muni, ano),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS alpha4_saude (
    code_muni   TEXT NOT NULL,
    ano         INTEGER NOT NULL,
    custo_rs    REAL NOT NULL,          -- R$ SUS anual
    n_internacoes INTEGER,
    fonte       TEXT,
    PRIMARY KEY (code_muni, ano),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS alpha5_ecossistemas (
    code_muni              TEXT NOT NULL,
    ano                    INTEGER NOT NULL,
    ha_manguezal           REAL DEFAULT 0,
    ha_recife              REAL DEFAULT 0,
    ha_restinga            REAL DEFAULT 0,
    valor_teeb_rs          REAL NOT NULL,   -- R$ anual na base ATIVA
    valor_teeb_global_rs   REAL,            -- R$ anual base global (Costanza 2014)
    valor_teeb_brasil_rs   REAL,            -- R$ anual base brasil (CCARBON/USP)
    fonte                  TEXT,
    PRIMARY KEY (code_muni, ano),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS beta_receitas_lgaf (
    ano         INTEGER PRIMARY KEY,
    valor_rs    REAL NOT NULL,
    fonte       TEXT
);

CREATE TABLE IF NOT EXISTS chi_custos_lgaf (
    ano         INTEGER PRIMARY KEY,
    valor_rs    REAL NOT NULL,
    fonte       TEXT
);

CREATE TABLE IF NOT EXISTS parametros (
    chave       TEXT PRIMARY KEY,
    valor       REAL NOT NULL,
    valor_texto TEXT,                      -- para parametros nao-numericos (ex: alpha5_base)
    unidade     TEXT,
    descricao   TEXT
);

CREATE TABLE IF NOT EXISTS eventos (
    id_evento       INTEGER PRIMARY KEY AUTOINCREMENT,
    data_evento     TEXT NOT NULL,      -- YYYY-MM-DD
    lon             REAL NOT NULL,
    lat             REAL NOT NULL,
    raio_km         REAL NOT NULL,
    foi_poluente    INTEGER NOT NULL,   -- 0/1
    descricao       TEXT,
    registrado_em   TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS eventos_municipios (
    id_evento   INTEGER NOT NULL,
    code_muni   TEXT NOT NULL,
    fracao      REAL DEFAULT 1.0,       -- fator de proporcao (0 a 1)
    PRIMARY KEY (id_evento, code_muni),
    FOREIGN KEY (id_evento) REFERENCES eventos(id_evento),
    FOREIGN KEY (code_muni) REFERENCES municipios_costeiros(code_muni)
);

CREATE TABLE IF NOT EXISTS resultados (
    id_evento       INTEGER PRIMARY KEY,
    alpha1_rs       REAL,
    alpha2_rs       REAL,
    alpha3_rs       REAL,
    alpha4_rs       REAL,
    alpha5_rs       REAL,
    beta_rs         REAL,
    chi_rs          REAL,
    k_aplicado      REAL,
    icseiom_rs      REAL,
    calculado_em    TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_evento) REFERENCES eventos(id_evento)
);

CREATE TABLE IF NOT EXISTS metadados_atualizacao (
    fonte                      TEXT PRIMARY KEY,   -- chave tecnica (ex: ibama_sifisc)
    ultima_safra               TEXT,
    atualizado_em              TEXT,
    url                        TEXT,               -- url do dado bruto
    observacoes                TEXT,
    nome_humano                TEXT,               -- nome legivel pra UI
    orgao                      TEXT,               -- IBGE, IBAMA, BCB, ANP, etc.
    url_portal                 TEXT,               -- pagina de entrada do portal
    descricao_uso              TEXT,               -- como o dado eh usado no ICSEIOM
    script                     TEXT,               -- script que popula
    observacoes_metodologicas  TEXT                -- decisoes metodologicas relevantes
);

CREATE INDEX IF NOT EXISTS idx_mun_uf ON municipios_costeiros(uf);
CREATE INDEX IF NOT EXISTS idx_a1_ano ON alpha1_multa_ambiental(ano);
CREATE INDEX IF NOT EXISTS idx_a2_ano ON alpha2_pesca(ano);
CREATE INDEX IF NOT EXISTS idx_a3_ano ON alpha3_turismo(ano);
CREATE INDEX IF NOT EXISTS idx_a4_ano ON alpha4_saude(ano);
CREATE INDEX IF NOT EXISTS idx_a5_ano ON alpha5_ecossistemas(ano);
"""

def main():
    con = sqlite3.connect(DB)
    con.executescript(SCHEMA)
    con.commit()
    n = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    con.close()
    print(f"[OK] Banco criado em {DB}")
    print(f"     Tabelas: {', '.join(t[0] for t in n)}")

if __name__ == "__main__":
    main()
