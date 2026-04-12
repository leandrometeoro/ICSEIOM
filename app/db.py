"""Acesso ao SQLite."""
import sqlite3
from contextlib import contextmanager
from .config import DB_PATH

# Colunas adicionadas em migracoes posteriores. Cada entry: (tabela, coluna, tipo).
# Aplicadas idempotentemente no primeiro get_conn da vida do processo.
_MIGRATIONS = [
    ("metadados_atualizacao", "nome_humano", "TEXT"),
    ("metadados_atualizacao", "orgao", "TEXT"),
    ("metadados_atualizacao", "url_portal", "TEXT"),
    ("metadados_atualizacao", "descricao_uso", "TEXT"),
    ("metadados_atualizacao", "script", "TEXT"),
    ("metadados_atualizacao", "observacoes_metodologicas", "TEXT"),
    # Valoracao dupla do alpha5: Costanza 2014 (global) vs CCARBON/USP (brasil).
    # `valor_teeb_rs` continua sendo a coluna "ativa" (espelha a base atual),
    # para compatibilidade com as queries existentes no main.py.
    ("alpha5_ecossistemas", "valor_teeb_global_rs", "REAL"),
    ("alpha5_ecossistemas", "valor_teeb_brasil_rs", "REAL"),
    # Parametro de texto (alpha5_base='global'|'brasil') precisa de coluna
    # textual, ja que parametros.valor e REAL NOT NULL.
    ("parametros", "valor_texto", "TEXT"),
]
_migrated = False


def _ensure_migrations(con: sqlite3.Connection) -> None:
    global _migrated
    if _migrated:
        return
    for tabela, coluna, tipo in _MIGRATIONS:
        existing = {r[1] for r in con.execute(f"PRAGMA table_info({tabela})")}
        if coluna not in existing:
            con.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")
    con.commit()
    _migrated = True


def get_conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    _ensure_migrations(con)
    return con


@contextmanager
def conn_ctx():
    con = get_conn()
    try:
        yield con
        con.commit()
    finally:
        con.close()


def query_all(sql: str, params: tuple = ()) -> list[dict]:
    with conn_ctx() as con:
        return [dict(r) for r in con.execute(sql, params).fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict | None:
    with conn_ctx() as con:
        row = con.execute(sql, params).fetchone()
        return dict(row) if row else None
