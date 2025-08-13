import pyodbc
import pandas as pd
import time
import logging
import requests
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

def insert_customers(df: pd.DataFrame):
    """
    Inserta solo clientes que NO existan ya en la tabla STG.customers.
    """
    conn, cursor = get_connection()
    try:
        # Asegurarse de que las columnas booleanas y numéricas sean manejadas correctamente
        df['customer_active'] = df['customer_active'].astype(bool)
        df['customer_vat_responsible'] = df['customer_vat_responsible'].astype(bool)

        df = df.where(pd.notnull(df), None)

        # Filtrar solo clientes nuevos (evitar duplicados)
        cursor.execute("SELECT customer_id FROM STG.customers")
        db_ids = set(row[0] for row in cursor.fetchall())
        nuevos_df = df[~df['customer_id'].isin(db_ids)]

        if nuevos_df.empty:
            print("[STG.customers] No hay clientes nuevos para insertar.")
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
            print(f"[STG.customers] {len(data)} clientes nuevos insertados.")
    except Exception as e:
        print(f"Error insertando en STG.customers: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_products(df: pd.DataFrame):
    """
    Inserta solo productos nuevos en STG.products con datos extendidos.
    Convierte valores numéricos y controla precisión para SQL Server.
    """
    conn, cursor = get_connection()
    try:
        # Limpiar NaN -> None
        df = df.where(pd.notnull(df), None)

        # Evitar duplicados
        cursor.execute("SELECT product_id FROM STG.products")
        db_ids = set(str(row[0]) for row in cursor.fetchall())
        nuevos_df = df[~df['product_id'].isin(db_ids)]

        if nuevos_df.empty:
            print("[STG.products] No hay productos nuevos para insertar.")
            return

        # Función para limpieza de decimales
        def safe_float(val):
            try:
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    return None
                return round(float(val), 2)
            except (ValueError, TypeError):
                return None

        # Normalizar numéricos
        nuevos_df['product_price'] = nuevos_df['product_price'].apply(lambda x: safe_float(x) or 0.0)
        nuevos_df['product_stock'] = nuevos_df['product_stock'].apply(lambda x: safe_float(x) or 0.0)

        # SQL con todos los campos
        SQL = """
            INSERT INTO STG.products (
                product_id, product_code, product_name, product_price,
                product_stock, product_status, category_name, product_type,
                tax_classification, tax_included, unit_label, brand,
                created_date, last_updated, has_stock_control, warehouse_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data = [
            (
                row.product_id,
                row.product_code,
                row.product_name,
                row.product_price,
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
            for row in nuevos_df.itertuples(index=False)
        ]

        cursor.executemany(SQL, data)
        conn.commit()
        print(f"[STG.products] {len(data)} productos nuevos insertados.")

    except Exception as e:
        print(f"[ERROR] Insertando en STG.products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    if pd.isna(val) or val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d")
    except:
        return None

def safe_get(data, *keys):
    """Extrae un valor anidado de forma segura"""
    for key in keys:
        if data is None:
            return None
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data

def insert_invoices(df: pd.DataFrame):
    if df.empty:
        print("[STG.invoices] No hay datos para procesar.")
        return

    # 🔹 Aseguramos que invoice_id no sea nulo antes de comparar
    df["invoice_id"] = df["invoice_id"].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else str(uuid.uuid4()))
    df["item_id"] = df["item_id"].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else "")

    conn, cursor = get_connection()
    try:
        # 1️⃣ Obtener registros existentes (tratando NULL como "")
        cursor.execute("SELECT ISNULL(invoice_id,''), ISNULL(item_id,'') FROM STG.invoices")
        existentes = set((str(row[0]).strip(), str(row[1]).strip()) for row in cursor.fetchall())

        # 2️⃣ Filtrar solo los nuevos
        nuevos = df[~df.apply(lambda x: (str(x["invoice_id"]).strip(), str(x["item_id"]).strip()) in existentes, axis=1)]

        if nuevos.empty:
            print("[STG.invoices] ✅ No hay registros nuevos para insertar.")
            return

        # 3️⃣ Insertar los nuevos
        for _, row in nuevos.iterrows():
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
                row.get("invoice_id"),
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
        print(f"[STG.invoices] ✅ Registros nuevos insertados: {len(nuevos)}")

    except Exception as e:
        print("Error insertando facturas:", e)
    finally:
        cursor.close()
        conn.close()