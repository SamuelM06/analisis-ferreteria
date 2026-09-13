"""
Capa de compatibilidad para Api/conexion.py.
Delega en la nueva arquitectura modular segura (config.settings y src.database).
"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import settings
from src.database.connection import get_connection, get_raw_connection
from src.database.repository import insert_customers, insert_products, insert_invoices
from src.utils.helpers import safe_date, safe_float, safe_get

# Variables expuestas dinámicamente para compatibilidad
server = settings.DB_SERVER
database = settings.DB_NAME
conn_str = settings.connection_string

__all__ = [
    "server",
    "database",
    "conn_str",
    "get_connection",
    "get_raw_connection",
    "insert_customers",
    "insert_products",
    "insert_invoices",
    "safe_date",
    "safe_float",
    "safe_get",
]