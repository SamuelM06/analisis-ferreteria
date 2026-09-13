import pyodbc
from contextlib import contextmanager
from typing import Generator, Tuple
from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("DatabaseConnection")

def get_raw_connection() -> pyodbc.Connection:
    """Retorna una nueva conexión activa con SQL Server."""
    conn_str = settings.connection_string
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        logger.error(f"Fallo al conectar con SQL Server ({settings.DB_SERVER}/{settings.DB_NAME}): {e}")
        raise

def get_connection() -> Tuple[pyodbc.Connection, pyodbc.Cursor]:
    """Retorna una tupla (conn, cursor) para compatibilidad con código existente."""
    conn = get_raw_connection()
    cursor = conn.cursor()
    return conn, cursor

@contextmanager
def db_session() -> Generator[Tuple[pyodbc.Connection, pyodbc.Cursor], None, None]:
    """Context manager para gestionar automáticamente commit, rollback y cierre de conexión."""
    conn = get_raw_connection()
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error en transacción de base de datos. Se ejecutó rollback: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def test_connection() -> bool:
    """Valida la conectividad con la base de datos."""
    try:
        with db_session() as (_, cursor):
            cursor.execute("SELECT 1 AS test")
            row = cursor.fetchone()
            if row and row[0] == 1:
                logger.info(f"Conexión exitosa a SQL Server en '{settings.DB_SERVER}', base de datos: '{settings.DB_NAME}'.")
                return True
        return False
    except Exception as e:
        logger.warning(f"No se pudo conectar a la base de datos: {e}")
        return False
