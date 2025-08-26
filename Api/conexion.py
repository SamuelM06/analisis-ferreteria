import pyodbc
import pandas as pd
import time
import logging
import numpy as np
import math
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import uuid

logging.basicConfig(
    filename="errores_invoices.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

server = 'LAPTOP-EIDER\\SQLDEV2022'
database = 'ferreteria_db'   

#conexion SQL Server
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
    'Encrypt=no;'
    'TrustServerCertificate=yes;'
)

def get_connection():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor

def safe_date(val):
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d")
    except:
        return None

def safe_float(val):
    if pd.isna(val) or val is None or val == '':
        return None
    try:
        # Check for both Pandas `nan` and Python's `math.nan`
        if isinstance(val, float) and math.isnan(val):
            return None
        return round(float(val), 2)
    except (ValueError, TypeError):
        return None

def safe_get(data, *keys):
    """Extrae un valor anidado de forma segura, manejando diccionarios y listas."""
    for key in keys:
        if data is None:
            return None
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and isinstance(key, int): # <--- CORRECCIÓN AQUÍ
            try:
                data = data[key]
            except IndexError:
                return None
        else:
            return None
    return data

def insert_products(df: pd.DataFrame):
    """
    Inserta solo productos nuevos en STG.products con datos extendidos.
    """
    if df.empty:
        print("[STG.products] ⚠ No hay datos para insertar.")
        return

    conn, cursor = get_connection()
    try:
        df = df.where(pd.notnull(df), None)
        df['product_id'] = df['product_id'].astype(str)

        cursor.execute("SELECT product_id FROM STG.products")
        db_ids = set(str(row[0]) for row in cursor.fetchall())

        nuevos_df = df[~df['product_id'].isin(db_ids)].copy()
        print(f"[DEBUG] 📦 Productos descargados: {len(df)} registros totales.")

        if nuevos_df.empty:
            print("[STG.products] ✅ No hay productos nuevos para insertar.")
            return

        def safe_float(val):
            if val is None or val == '' or (isinstance(val, float) and math.isnan(val)):
                return 0  # <--- CAMBIO CLAVE: Ahora devuelve 0 en lugar de None
            try:
                return round(float(val), 2)
            except (ValueError, TypeError):
                return 0

        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        def safe_bit(val):
            if val is None:
                return None
            return 1 if val else 0
        
        nuevos_df['product_price'] = nuevos_df['product_price'].apply(safe_float)
        nuevos_df['product_cost'] = nuevos_df['product_cost'].apply(safe_float)
        nuevos_df['product_stock'] = nuevos_df['product_stock'].apply(safe_int)
        nuevos_df['warehouse_count'] = nuevos_df['warehouse_count'].apply(safe_int)
        nuevos_df['has_stock_control'] = nuevos_df['has_stock_control'].apply(safe_bit)
        nuevos_df['tax_included'] = nuevos_df['tax_included'].apply(safe_bit)

        SQL = """
            INSERT INTO STG.products (
                product_id, product_code, product_name, product_price,
                product_cost, product_stock, product_status, category_name,
                product_type, tax_classification, tax_included, unit_label,
                brand, created_date, last_updated, has_stock_control, warehouse_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        inserted_count = 0
        # CORRECCIÓN FINAL: Iteramos y manejamos nan directamente en el bucle
        for row in nuevos_df.itertuples(index=False):
            # Limpiamos el valor de product_price directamente en la tupla
            clean_price = None if isinstance(row.product_price, float) and math.isnan(row.product_price) else row.product_price
            
            try:
                cursor.execute(
                    SQL,
                    (
                        row.product_id,
                        row.product_code,
                        row.product_name,
                        clean_price, # Usamos el valor limpio aquí
                        row.product_cost,
                        row.product_stock,
                        row.product_status,
                        row.category_name,
                        row.product_type,
                        row.tax_classification,
                        row.tax_included,
                        row.unit_label,
                        row.brand,
                        row.created_date,
                        row.last_updated,
                        row.has_stock_control,
                        row.warehouse_count
                    )
                )
                inserted_count += 1
            except pyodbc.Error as e:
                print(f"[ERROR] ❌ Fallo al insertar la fila: {row}")
                print(f"[ERROR] ❌ Detalles del error: {e}")
                raise

        conn.commit()
        print(f"[STG.products] 🚀 {inserted_count} productos nuevos insertados.")

    except Exception as e:
        print(f"[ERROR] ❌ Insertando en STG.products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_customers(df: pd.DataFrame):
    """
    Inserta solo clientes que NO existan ya en la tabla STG.customers.
    """
    conn, cursor = get_connection()
    try:
        # Normalizar columnas y evitar NaN
        df['customer_active'] = df['customer_active'].astype(bool)
        df['customer_vat_responsible'] = df['customer_vat_responsible'].astype(bool)
        df = df.where(pd.notnull(df), None)

        # Convertir a string para evitar falsos duplicados por tipos
        df['customer_id'] = df['customer_id'].astype(str)

        # Cargar IDs existentes
        cursor.execute("SELECT customer_id FROM STG.customers")
        db_ids = set(str(row[0]) for row in cursor.fetchall())

        # Filtrar solo nuevos
        nuevos_df = df[~df['customer_id'].isin(db_ids)]

        print(f"[DEBUG] 📊 Clientes descargados: {len(df)} registros totales.")
        if nuevos_df.empty:
            print("[STG.customers] ✅ No hay clientes nuevos para insertar.")
        else:
            SQL = """
                INSERT INTO STG.customers (
                    customer_id, customer_type, customer_identification, customer_name, customer_active,
                    customer_vat_responsible, customer_id_type_name, customer_fiscal_responsibility_name,
                    customer_address, customer_city_name, customer_state_name, customer_phone_number, customer_email
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            data = [
                (
                    row.customer_id, row.customer_type, row.customer_identification, row.customer_name, row.customer_active,
                    row.customer_vat_responsible, row.customer_id_type_name, row.customer_fiscal_responsibility_name,
                    row.customer_address, row.customer_city_name, row.customer_state_name, row.customer_phone_number, row.customer_email
                )
                for row in nuevos_df.itertuples(index=False)
            ]
            cursor.executemany(SQL, data)
            conn.commit()
            print(f"[STG.customers] 🚀 {len(data)} clientes nuevos insertados.")

    except Exception as e:
        print(f"[ERROR] ❌ Insertando en STG.customers: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_invoices(df: pd.DataFrame):
    conn, cursor = get_connection()
    try:
        for _, row in df.iterrows():
            # Si no tiene invoice_id, generar uno temporal
            invoice_id = row.get("invoice_id")
            if pd.isna(invoice_id) or str(invoice_id).strip() == "":
                invoice_id = str(uuid.uuid4())

            cursor.execute("""
                INSERT INTO STG.invoices (
                    invoice_id, item_id, invoice_number, invoice_date, status,
                    created_date, customer_id, customer_name, customer_email, 
                    customer_phone, seller_name, warehouse_name,
                    product_id, item_description, item_quantity, item_price,
                    item_total, item_tax, item_discount, reference_number,
                    document_type, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                invoice_id,
                row.get("item_id"),
                row.get("invoice_number"),
                row.get("invoice_date"),
                row.get("status"),
                row.get("created_date"),
                row.get("customer_id"),
                row.get("customer_name"),
                row.get("customer_email"),
                row.get("customer_phone"),
                row.get("seller_name"),
                row.get("warehouse_name"),
                row.get("product_id"),
                row.get("item_description"),
                row.get("item_quantity"),
                row.get("item_price"),
                row.get("item_total"),
                row.get("item_tax"),
                row.get("item_discount"),
                row.get("reference_number"),
                row.get("document_type"),
                row.get("notes")
            ))
        conn.commit()
    except Exception as e:
        print("Error insertando facturas:", e)
    finally:
        cursor.close()
        conn.close()