import math
from datetime import datetime
import pandas as pd

def safe_get(data, *keys):
    """
    Extrae un valor anidado de forma segura, manejando diccionarios y listas indexadas.
    """
    for key in keys:
        if data is None:
            return None
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and isinstance(key, int):
            try:
                data = data[key]
            except IndexError:
                return None
        else:
            return None
    return data

def safe_date(val):
    """Convierte un valor a objeto datetime seguro o None."""
    if pd.isna(val) or val is None or str(val).strip() == "":
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

def safe_float(val, default=None):
    """Convierte a float redondeado a 2 decimales sin errores de NaN."""
    if pd.isna(val) or val is None or val == "":
        return default
    try:
        if isinstance(val, float) and math.isnan(val):
            return default
        return round(float(val), 2)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=None):
    """Convierte a int seguro sin errores de conversión."""
    if pd.isna(val) or val is None or val == "":
        return default
    try:
        if isinstance(val, float) and math.isnan(val):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_bit(val):
    """Convierte a 1 o 0 para columnas tipo BIT de SQL Server."""
    if val is None or pd.isna(val):
        return None
    return 1 if bool(val) else 0
