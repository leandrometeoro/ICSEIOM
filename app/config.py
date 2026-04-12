"""Configurações lidas do ambiente."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("ICSEIOM_DB", ROOT / "db" / "icseiom.db"))

SECRET_KEY = os.getenv("ICSEIOM_SECRET", "change-me-in-production-please")

ADMIN_USER = os.getenv("ICSEIOM_ADMIN_USER", "admin")
# Password default = "icseiom" (override via env ICSEIOM_ADMIN_PASSWORD)
ADMIN_PASSWORD = os.getenv("ICSEIOM_ADMIN_PASSWORD", "icseiom")

APP_TITLE = "ICSEIOM — Índice de Custo Socioambiental Evitado por Incidentes com Óleo no Mar"
APP_SHORT = "ICSEIOM"
ORG = "LGAF · IEAPM · Marinha do Brasil"
