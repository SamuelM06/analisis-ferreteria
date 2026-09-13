import time
import requests
import pandas as pd
from config.settings import settings
from src.utils.helpers import safe_get
from src.utils.logger import get_logger

logger = get_logger("ProductExtractor")

def get_products(access_token: str, max_retries: int = 5, wait_time: int = 60) -> pd.DataFrame:
    """
    Descarga todos los productos de Siigo con datos extendidos y columnas normalizadas.
    """
    base_url = f"{settings.SIIGO_API_BASE_URL}/products"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": settings.SIIGO_PARTNER_ID
    }

    all_data = []
    page = 1

    logger.info("Iniciando extracción de productos...")

    while True:
        api_url = f"{base_url}?page={page}"
        retries = 0

        while retries <= max_retries:
            try:
                response = requests.get(api_url, headers=headers, timeout=45)
            except requests.RequestException as e:
                logger.warning(f"Error de red en página {page}: {e}. Reintentando...")
                retries += 1
                time.sleep(5)
                continue

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                logger.warning(f"Límite de peticiones (429) en página {page}. Esperando {wait_time}s...")
                time.sleep(wait_time)
                retries += 1
            else:
                logger.error(f"Error HTTP {response.status_code} en página {page}: {response.text}")
                return pd.DataFrame(all_data)

        if retries > max_retries:
            logger.error("Máximo de reintentos alcanzado. Finalizando extracción parcial de productos.")
            return pd.DataFrame(all_data)

        payload = response.json()
        results = payload.get("results", [])
        if not results:
            break

        for producto in results:
            product_price = safe_get(producto, "prices", 0, "price_list", 0, "value")
            product_stock = safe_get(producto, "available_quantity")
            product_cost = None
            brand = safe_get(producto, "additional_fields", "brand")
            unit_label = safe_get(producto, "unit", "unit_label")

            row = {
                "product_id": producto.get("id"),
                "product_code": producto.get("code", ""),
                "product_name": producto.get("name", ""),
                "product_price": product_price,
                "product_cost": product_cost,
                "product_stock": product_stock,
                "product_status": producto.get("active"),
                "category_name": safe_get(producto, "account_group", "name"),
                "product_type": safe_get(producto, "type"),
                "tax_classification": safe_get(producto, "tax_classification"),
                "tax_included": safe_get(producto, "tax_included"),
                "unit_label": unit_label,
                "brand": brand,
                "created_date": safe_get(producto, "metadata", "created"),
                "last_updated": safe_get(producto, "metadata", "last_updated"),
                "has_stock_control": producto.get("stock_control"),
                "warehouse_count": len(producto.get("warehouses", []))
            }
            all_data.append(row)

        page += 1

    logger.info(f"Extracción de productos finalizada. Total registros: {len(all_data)}")

    columns_order = [
        "product_id", "product_code", "product_name", "product_price",
        "product_cost", "product_stock", "product_status", "category_name",
        "product_type", "tax_classification", "tax_included", "unit_label",
        "brand", "created_date", "last_updated", "has_stock_control",
        "warehouse_count"
    ]

    return pd.DataFrame(all_data, columns=columns_order)

# Alias para compatibilidad
get_productos = get_products
