import pyodbc
import pandas as pd
import time
import logging
import requests

logging.basicConfig(filename="errores_invoices.log", level=logging.WARNING,
                    format="%(asctime)s - %(levelname)s - %(message)s")

server = 'LAPTOP-EIDER\\SQLDEV2022'
database = 'ferreteria_db'   

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
    Limpia customer_name (y otros campos si quieres) SOLO si es lista o None.
    """
    conn, cursor = get_connection()
    try:
        # LIMPIEZA SOLO customer_name: si es lista, une; si None, convierte a vacío
        def simple_clean(val):
            if isinstance(val, list):
                return " ".join(str(x) for x in val if x is not None)
            if val is None:
                return ""
            return str(val)
        
        df["customer_name"] = df["customer_name"].apply(simple_clean)
        # Si quieres aplicar limpieza a otros campos de texto (opcional), agrega igual lógica aquí
        
        df = df.where(pd.notnull(df), None)

        # Filtra solo clientes nuevos (evita duplicados)
        cursor.execute("SELECT customer_id FROM STG.customers")
        db_ids = set(row[0] for row in cursor.fetchall())
        nuevos_df = df[~df['customer_id'].isin(db_ids)]

        if nuevos_df.empty:
            print("[STG.customers] No hay clientes nuevos para insertar.")
        else:
            SQL = """
                INSERT INTO STG.customers (
                    customer_id, customer_name, customer_identification, customer_phone,
                    customer_email, customer_type
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """
            data = [
                (
                    row.customer_id, row.customer_name, row.customer_identification, 
                    row.customer_phone, row.customer_email, row.customer_type
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
    Inserta solo productos que NO existan ya en la tabla STG.products (por product_id).
    Limpia tipos: Texto puro para campos de texto, números válidos para DECIMAL(10,2)/(5,2).
    """
    conn, cursor = get_connection()
    try:
        # LIMPIEZA DE TEXTOS (garantiza string, nunca None)
        for col in ["product_code", "product_name", "product_reference", "product_description"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: "" if x is None else str(x))
        
        # LIMPIEZA Y REDONDEO DE DECIMALES
        def safe_decimal(val, decimals):
            try:
                if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
                    return None
                # Si el valor viene con coma o símbolo
                f = float(str(val).replace(",", ".").replace("$", "").strip())
                return round(f, decimals)
            except Exception:
                return None

        if "product_price" in df.columns:
            df["product_price"] = df["product_price"].apply(lambda v: safe_decimal(v, 2))
        if "product_tax" in df.columns:
            df["product_tax"] = df["product_tax"].apply(lambda v: safe_decimal(v, 2))

        df = df.where(pd.notnull(df), None)

        cursor.execute("SELECT product_id FROM STG.products")
        db_ids = set(row[0] for row in cursor.fetchall())
        nuevos_df = df[~df['product_id'].isin(db_ids)]

        if nuevos_df.empty:
            print("[STG.products] No hay productos nuevos para insertar.")
        else:
            SQL = """
                INSERT INTO STG.products (
                    product_id, product_code, product_name, product_reference,
                    product_description, product_price, product_tax
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            data = [
                (
                    row.product_id, row.product_code, row.product_name, row.product_reference, 
                    row.product_description, row.product_price, row.product_tax
                )
                for row in nuevos_df.itertuples(index=False)
            ]
            cursor.executemany(SQL, data)
            conn.commit()
            print(f"[STG.products] {len(data)} productos nuevos insertados.")
    except Exception as e:
        print(f"Error insertando en STG.products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def fetch_invoices_page(url, headers, retries=3, delay=5):
    """
    Intenta obtener una página de facturas, reintentando si hay error HTTP 500.
    """
    for intento in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 500:
                print(f"[WARN] Error 500 en {url}, intento {intento} de {retries}. Reintentando en {delay}s...")
                time.sleep(delay)
            else:
                response.raise_for_status()
        except Exception as e:
            print(f"[ERROR] Fallo conexión al obtener página: {e}")
            time.sleep(delay)
    print(f"[ERROR] No se pudo obtener la página después de {retries} intentos: {url}")
    return None

def insert_invoices(df: pd.DataFrame):
    """
    Inserta solo facturas nuevas por (invoice_id, item_id) en STG.invoices.
    Limpia datos problemáticos para que sean compatibles con DECIMAL según SQL Server.
    """
    conn, cursor = get_connection()
    try:
        from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

        def clean_decimal(value, precision, max_digits, col_name):
            try:
                if value is None or pd.isna(value):
                    return None
                value = str(value).strip()
                if value == "":
                    return None

                num = Decimal(value)
                quant = Decimal("1." + "0" * precision)
                num = num.quantize(quant, rounding=ROUND_HALF_UP)

                entero_max = max_digits - precision
                max_val = Decimal("9" * entero_max + "." + "9" * precision)
                min_val = -max_val
                if num > max_val or num < min_val:
                    logging.warning(f"Valor fuera de rango en columna {col_name}: {value}")
                    return None

                return float(num)
            except (InvalidOperation, ValueError, TypeError) as ex:
                logging.warning(f"Valor inválido en columna {col_name}: {value} - Error: {ex}")
                return None

        column_specs = {
            "invoice_total":  (2, 10),
            "payment_value":  (2, 10),
            "item_quantity":  (6, 18),
            "item_price":     (2, 10),
            "item_total":     (2, 10)
        }

        for col, (prec, digits) in column_specs.items():
            if col in df.columns:
                df[col] = df[col].apply(lambda v: clean_decimal(v, prec, digits, col))

        df = df.where(pd.notnull(df), None)

        cursor.execute("SELECT invoice_id, item_id FROM STG.invoices")
        db_ids = set((row[0], row[1]) for row in cursor.fetchall())

        df["pair_key"] = list(zip(df["invoice_id"], df["item_id"]))
        nuevos_df = df[~df["pair_key"].isin(db_ids)].copy()
        nuevos_df.drop(columns=["pair_key"], inplace=True)

        if nuevos_df.empty:
            print("[STG.invoices] No hay facturas nuevas para insertar.")
            return

        SQL = """
            INSERT INTO STG.invoices (
                invoice_id, invoice_number, invoice_date, invoice_total,
                seller_id, customer_id, customer_identification,
                payment_method, payment_value, item_id, item_code,
                item_quantity, item_price, item_description, item_total
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Insertar fila por fila para detectar errores y evitar abortos masivos
        inserted_count = 0
        for row in nuevos_df.itertuples(index=False):
            try:
                cursor.execute(SQL, tuple(row))
                inserted_count += 1
            except Exception as e:
                logging.error(f"Error insertando factura invoice_id={row.invoice_id}, item_id={row.item_id}: {e}")
                continue

        conn.commit()
        print(f"[STG.invoices] {inserted_count} nuevas facturas insertadas.")

    except Exception as e:
        print(f"Error insertando en STG.invoices: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
