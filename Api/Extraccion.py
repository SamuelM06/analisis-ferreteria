import sys
from pathlib import Path
from datetime import date, timedelta
import requests
import json
import pandas as pd

# Asegurar importación del directorio raíz
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("Extraccion_Script")

def main():
    logger.info("Iniciando script de prueba de extracción...")

    # Validar credenciales de entorno
    settings.validate()

    url_auth = settings.SIIGO_AUTH_URL
    body_auth = {
        "username": settings.SIIGO_USERNAME,
        "access_key": settings.SIIGO_ACCESS_KEY
    }
    headers_auth = {
        "Content-Type": "application/json"
    }

    response = requests.post(url_auth, headers=headers_auth, json=body_auth, timeout=30)
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data["access_token"]
        logger.info("TOKEN obtenido correctamente.")
    else:
        logger.error(f"Error de autenticación: {response.status_code} - {response.text}")
        sys.exit(1)

    # Consulta con rango de fecha
    Date_last = date.today() - timedelta(days=1)
    Looking_Type = "created_end"
    reference_type = "invoices"

    api_url = f"{settings.SIIGO_API_BASE_URL}/{reference_type}?{Looking_Type}={Date_last}"
    headers_api = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": settings.SIIGO_PARTNER_ID
    }

    response_api = requests.get(api_url, headers=headers_api, timeout=30)
    if response_api.status_code == 200:
        api = response_api.json()
        result_data = api.get("results", [])
        logger.info(f"Registros obtenidos: {len(result_data)}")
        flattened_data = []

        for invoice in result_data:
            invoice_id = invoice.get("id")
            invoice_number = invoice.get("number")
            invoice_date = invoice.get("date")
            invoice_total = invoice.get("total")
            invoice_seller = invoice.get("seller")

            customer_data = invoice.get("customer", {})
            customer_id = customer_data.get("id")
            customer_identification = customer_data.get("identification")

            payments = invoice.get("payments", [])
            payment_method = payments[0]["name"] if payments else None
            payment_value = payments[0]["value"] if payments else None

            items = invoice.get("items", [])
            if items:
                for item in items:
                    row = {
                        "invoice_id": invoice_id,
                        "invoice_number": invoice_number,
                        "invoice_date": invoice_date,
                        "invoice_total": invoice_total,
                        "seller_id": invoice_seller,
                        "customer_id": customer_id,
                        "customer_identification": customer_identification,
                        "payment_method": payment_method,
                        "payment_value": payment_value,
                        "item_id": item.get("id"),
                        "item_code": item.get("code"),
                        "item_quantity": item.get("quantity"),
                        "item_price": item.get("price"),
                        "item_description": item.get("description"),
                        "item_total": item.get("total")
                    }
                    flattened_data.append(row)
            else:
                row = {
                    "invoice_id": invoice_id,
                    "invoice_number": invoice_number,
                    "invoice_date": invoice_date,
                    "invoice_total": invoice_total,
                    "seller_id": invoice_seller,
                    "customer_id": customer_id,
                    "customer_identification": customer_identification,
                    "payment_method": payment_method,
                    "payment_value": payment_value,
                    "item_id": None,
                    "item_code": None,
                    "item_quantity": None,
                    "item_price": None,
                    "item_description": None,
                    "item_total": None
                }
                flattened_data.append(row)

        df = pd.DataFrame(flattened_data)
        logger.info(f"DataFrame generado exitosamente ({len(df)} filas).")
        print(df.head())
    else:
        logger.error(f"Error al consultar API: {response_api.status_code} - {response_api.text}")

if __name__ == "__main__":
    main()