import time
import uuid
import requests
import pandas as pd
from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("InvoiceExtractor")

def get_invoices(access_token: str, max_retries: int = 5, wait_time: int = 60) -> pd.DataFrame:
    """
    Descarga facturas e ítems desglosados desde Siigo API.
    """
    base_url = f"{settings.SIIGO_API_BASE_URL}/invoices"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": settings.SIIGO_PARTNER_ID
    }

    all_data = []
    page = 1

    logger.info("Iniciando extracción de facturas...")

    while True:
        api_url = f"{base_url}?page={page}&page_size=100"
        retries = 0

        while retries <= max_retries:
            try:
                response = requests.get(api_url, headers=headers, timeout=45)
            except requests.RequestException as e:
                logger.warning(f"Error de red en facturas página {page}: {e}. Reintentando...")
                retries += 1
                time.sleep(5)
                continue

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                logger.warning(f"Límite de peticiones (429) en facturas página {page}. Esperando {wait_time}s...")
                time.sleep(wait_time)
                retries += 1
            else:
                logger.error(f"Error HTTP {response.status_code} en facturas página {page}: {response.text}")
                return pd.DataFrame(all_data)

        if retries > max_retries:
            logger.error("Máximo de reintentos alcanzado en facturas. Finalizando extracción parcial.")
            return pd.DataFrame(all_data)

        payload = response.json()
        facturas = payload.get("results", [])
        if not facturas:
            break

        for factura in facturas:
            try:
                invoice_id = factura.get("id")
                invoice_number = factura.get("document_number") or factura.get("number")
                invoice_date = factura.get("date")
                status = factura.get("state") or factura.get("status")
                created_date = factura.get("created_at") or factura.get("date")

                customer_data = factura.get("customer") if isinstance(factura.get("customer"), dict) else {}
                customer_id = customer_data.get("id")
                customer_name = customer_data.get("name")
                customer_email = customer_data.get("email")
                customer_phone = customer_data.get("phone")

                seller_data = factura.get("seller") if isinstance(factura.get("seller"), dict) else {}
                seller_name = seller_data.get("name")

                warehouse_data = factura.get("warehouse") if isinstance(factura.get("warehouse"), dict) else {}
                warehouse_name = warehouse_data.get("name")

                reference_number = factura.get("reference_number")
                document_type = factura.get("document_type")
                notes = factura.get("observations")

                items = factura.get("items") if isinstance(factura.get("items"), list) else []
                if not items:
                    all_data.append({
                        "invoice_id": invoice_id,
                        "item_id": None,
                        "invoice_number": invoice_number,
                        "invoice_date": invoice_date,
                        "status": status,
                        "created_date": created_date,
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "customer_email": customer_email,
                        "customer_phone": customer_phone,
                        "seller_name": seller_name,
                        "warehouse_name": warehouse_name,
                        "product_id": None,
                        "item_description": None,
                        "item_quantity": 0.0,
                        "item_price": 0.0,
                        "item_total": 0.0,
                        "item_tax": 0.0,
                        "item_discount": 0.0,
                        "reference_number": reference_number,
                        "document_type": document_type,
                        "notes": notes
                    })
                    continue

                for item in items:
                    product_id = item.get("code")
                    item_id = item.get("id")
                    item_desc = item.get("description")
                    qty = item.get("quantity", 0.0)

                    price = 0.0
                    price_data = item.get("price")
                    if isinstance(price_data, dict):
                        val = price_data.get("value")
                        if val and not isinstance(val, str):
                            price = float(val)

                    total = item.get("total")

                    tax = 0.0
                    taxes = item.get("taxes")
                    if isinstance(taxes, list) and taxes:
                        first_tax = taxes[0]
                        if isinstance(first_tax, dict):
                            val_tax = first_tax.get("value")
                            if val_tax and not isinstance(val_tax, str):
                                tax = float(val_tax)

                    discount = item.get("discount")

                    all_data.append({
                        "invoice_id": invoice_id,
                        "item_id": item_id,
                        "invoice_number": invoice_number,
                        "invoice_date": invoice_date,
                        "status": status,
                        "created_date": created_date,
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "customer_email": customer_email,
                        "customer_phone": customer_phone,
                        "seller_name": seller_name,
                        "warehouse_name": warehouse_name,
                        "product_id": product_id,
                        "item_description": item_desc,
                        "item_quantity": qty if qty is not None else 0.0,
                        "item_price": price,
                        "item_total": total if total is not None else 0.0,
                        "item_tax": tax,
                        "item_discount": discount if discount is not None else 0.0,
                        "reference_number": reference_number,
                        "document_type": document_type,
                        "notes": notes
                    })

            except Exception as e:
                logger.error(f"Error procesando factura individual: {e}")

        page += 1

    logger.info(f"Extracción de facturas finalizada. Total ítems de facturas: {len(all_data)}")

    df = pd.DataFrame(all_data)
    if not df.empty:
        df["invoice_id"] = df["invoice_id"].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else str(uuid.uuid4())
        )
        df["item_id"] = df["item_id"].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else "0"
        )

    return df

# Alias para compatibilidad
get_facturas = get_invoices
