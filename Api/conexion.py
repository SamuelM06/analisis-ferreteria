import pyodbc
import pandas as pd

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
    conn, cursor = get_connection()
    SQL = """
        INSERT INTO STG.customers (
            customer_id, customer_name, customer_identification, customer_phone,
            customer_email, customer_type
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        data = [
            (
                row.customer_id, row.customer_name, row.customer_identification, 
                row.customer_phone, row.customer_email, row.customer_type
            )
            for row in df.itertuples(index=False)
        ]
        cursor.executemany(SQL, data)
        conn.commit()
        print(f"[STG.customers] {len(data)} filas insertadas.")
    except Exception as e:
        print(f"Error insertando en STG.customers: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_products(df: pd.DataFrame):
    conn, cursor = get_connection()
    SQL = """
        INSERT INTO STG.products (
            product_id, product_code, product_name, product_reference,
            product_description, product_price, product_tax
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        data = [
            (
                row.product_id, row.product_code, row.product_name, row.product_reference, 
                row.product_description, row.product_price, row.product_tax
            )
            for row in df.itertuples(index=False)
        ]
        cursor.executemany(SQL, data)
        conn.commit()
        print(f"[STG.products] {len(data)} filas insertadas.")
    except Exception as e:
        print(f"Error insertando en STG.products: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_invoices(df: pd.DataFrame):
    conn, cursor = get_connection()
    SQL = """
        INSERT INTO STG.invoices (
            invoice_id, invoice_number, invoice_date, invoice_total,
            seller_id, customer_id, customer_identification,
            payment_method, payment_value, item_id, item_code,
            item_quantity, item_price, item_description, item_total
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        data = [
            (
                row.invoice_id, row.invoice_number, row.invoice_date, row.invoice_total,
                row.seller_id, row.customer_id, row.customer_identification,
                row.payment_method, row.payment_value, row.item_id, row.item_code,
                row.item_quantity, row.item_price, row.item_description, row.item_total
            )
            for row in df.itertuples(index=False)
        ]
        cursor.executemany(SQL, data)
        conn.commit()
        print(f"[STG.invoices] {len(data)} filas insertadas.")
    except Exception as e:
        print(f"Error insertando en STG.invoices: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()