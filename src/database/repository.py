import math
import uuid
import pyodbc
import pandas as pd
from src.database.connection import db_session
from src.utils.helpers import safe_float, safe_int, safe_bit
from src.utils.logger import get_logger

logger = get_logger("DatabaseRepository")

def insert_products(df: pd.DataFrame) -> int:
    """
    Inserta solo productos nuevos en STG.products con datos extendidos.
    Retorna la cantidad de productos insertados.
    """
    if df.empty:
        logger.warning("[STG.products] No hay datos para procesar.")
        return 0

    with db_session() as (conn, cursor):
        df_clean = df.where(pd.notnull(df), None).copy()
        df_clean["product_id"] = df_clean["product_id"].astype(str)

        cursor.execute("SELECT product_id FROM STG.products")
        existing_ids = set(str(row[0]) for row in cursor.fetchall())

        nuevos_df = df_clean[~df_clean["product_id"].isin(existing_ids)].copy()
        logger.info(f"[STG.products] Productos extraídos: {len(df)} | Nuevos para insertar: {len(nuevos_df)}")

        if nuevos_df.empty:
            logger.info("[STG.products] No hay productos nuevos para insertar.")
            return 0

        # Normalización de tipos
        nuevos_df["product_price"] = nuevos_df["product_price"].apply(lambda v: safe_float(v, default=0.0))
        nuevos_df["product_cost"] = nuevos_df["product_cost"].apply(lambda v: safe_float(v, default=0.0))
        nuevos_df["product_stock"] = nuevos_df["product_stock"].apply(lambda v: safe_int(v, default=0))
        nuevos_df["warehouse_count"] = nuevos_df["warehouse_count"].apply(lambda v: safe_int(v, default=0))
        nuevos_df["has_stock_control"] = nuevos_df["has_stock_control"].apply(safe_bit)
        nuevos_df["tax_included"] = nuevos_df["tax_included"].apply(safe_bit)

        sql = """
            INSERT INTO STG.products (
                product_id, product_code, product_name, product_price,
                product_cost, product_stock, product_status, category_name,
                product_type, tax_classification, tax_included, unit_label,
                brand, created_date, last_updated, has_stock_control, warehouse_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        inserted_count = 0
        for row in nuevos_df.itertuples(index=False):
            clean_price = 0.0 if isinstance(row.product_price, float) and math.isnan(row.product_price) else row.product_price
            clean_cost = 0.0 if isinstance(row.product_cost, float) and math.isnan(row.product_cost) else row.product_cost

            try:
                cursor.execute(
                    sql,
                    (
                        str(row.product_id),
                        row.product_code,
                        row.product_name,
                        clean_price,
                        clean_cost,
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
                        row.warehouse_count,
                    )
                )
                inserted_count += 1
            except pyodbc.Error as e:
                logger.error(f"[STG.products] Fallo al insertar fila {row.product_id}: {e}")
                raise

        logger.info(f"[STG.products] {inserted_count} productos insertados exitosamente.")
        return inserted_count


def insert_customers(df: pd.DataFrame) -> int:
    """
    Inserta solo clientes que NO existan en STG.customers.
    Retorna la cantidad de clientes insertados.
    """
    if df.empty:
        logger.warning("[STG.customers] No hay datos de clientes para procesar.")
        return 0

    with db_session() as (conn, cursor):
        df_clean = df.where(pd.notnull(df), None).copy()
        df_clean["customer_id"] = df_clean["customer_id"].astype(str)
        df_clean["customer_active"] = df_clean["customer_active"].astype(bool)
        df_clean["customer_vat_responsible"] = df_clean["customer_vat_responsible"].astype(bool)

        cursor.execute("SELECT customer_id FROM STG.customers")
        existing_ids = set(str(row[0]) for row in cursor.fetchall())

        nuevos_df = df_clean[~df_clean["customer_id"].isin(existing_ids)]
        logger.info(f"[STG.customers] Clientes extraídos: {len(df)} | Nuevos para insertar: {len(nuevos_df)}")

        if nuevos_df.empty:
            logger.info("[STG.customers] No hay clientes nuevos para insertar.")
            return 0

        sql = """
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

        cursor.executemany(sql, data)
        logger.info(f"[STG.customers] {len(data)} clientes insertados exitosamente.")
        return len(data)


def insert_invoices(df: pd.DataFrame) -> int:
    """
    Inserta registros de facturas en STG.invoices.
    Retorna la cantidad de registros insertados.
    """
    if df.empty:
        logger.warning("[STG.invoices] No hay datos de facturas para procesar.")
        return 0

    with db_session() as (conn, cursor):
        sql = """
            INSERT INTO STG.invoices (
                invoice_id, item_id, invoice_number, invoice_date, status,
                created_date, customer_id, customer_name, customer_email, 
                customer_phone, seller_name, warehouse_name,
                product_id, item_description, item_quantity, item_price,
                item_total, item_tax, item_discount, reference_number,
                document_type, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """

        inserted_count = 0
        for _, row in df.iterrows():
            invoice_id = row.get("invoice_id")
            if pd.isna(invoice_id) or str(invoice_id).strip() == "":
                invoice_id = str(uuid.uuid4())

            cursor.execute(sql, (
                str(invoice_id),
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
            inserted_count += 1

        logger.info(f"[STG.invoices] {inserted_count} registros de facturas insertados exitosamente.")
        return inserted_count
