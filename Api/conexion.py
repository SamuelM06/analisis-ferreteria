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
    Inserta solo productos que NO existan ya en la tabla STG.products (por product_id).
    """
    conn, cursor = get_connection()
    try:
        # Asegurarse de que los valores nulos sean None
        df = df.where(pd.notnull(df), None)

        # Traer los IDs existentes para evitar duplicados
        cursor.execute("SELECT product_id FROM STG.products")
        db_ids = set(row[0] for row in cursor.fetchall())

        nuevos_df = df[~df['product_id'].isin(db_ids)]

        if nuevos_df.empty:
            print("[STG.products] No hay productos nuevos para insertar.")
        else:
            SQL = """
                INSERT INTO STG.products (
                    product_id, product_code, product_name, product_price, product_cost,
                    product_stock, product_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            data = [
                (
                    row.product_id, row.product_code, row.product_name, row.product_price, row.product_cost,
                    row.product_stock, row.product_status
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
