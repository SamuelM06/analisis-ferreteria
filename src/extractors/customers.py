import time
import requests
import pandas as pd
from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("CustomerExtractor")

def get_customers(access_token: str, max_retries: int = 5, wait_time: int = 60) -> pd.DataFrame:
    """
    Extrae los clientes desde Siigo API paginando y manejando reintentos (ej. HTTP 429).
    """
    base_url = f"{settings.SIIGO_API_BASE_URL}/customers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": settings.SIIGO_PARTNER_ID
    }

    all_data = []
    page = 1

    logger.info("Iniciando extracción de clientes...")

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
                logger.error(f"Error HTTP {response.status_code} al obtener página {page}: {response.text}")
                return pd.DataFrame(all_data)

        if retries > max_retries:
            logger.error("Máximo de reintentos alcanzado. Finalizando extracción parcial de clientes.")
            return pd.DataFrame(all_data)

        payload = response.json()
        results = payload.get("results", [])
        if not results:
            break

        for cliente in results:
            id_type = cliente.get("id_type") or {}
            city = cliente.get("city") or {}
            address_data = cliente.get("address") or {}
            phone_data = cliente.get("phones") or []
            fiscal_resp = cliente.get("fiscal_responsibilities") or []

            # Normalización del nombre
            raw_name = cliente.get("name")
            if isinstance(raw_name, list):
                customer_name = " ".join(str(x) for x in raw_name if x is not None)
            else:
                customer_name = str(raw_name) if raw_name is not None else ""

            phone_number = (
                phone_data[0].get("number")
                if phone_data and isinstance(phone_data, list) and phone_data[0]
                else None
            )

            fiscal_name = (
                fiscal_resp[0].get("name")
                if fiscal_resp and isinstance(fiscal_resp, list) and fiscal_resp[0]
                else None
            )

            row = {
                "customer_id": cliente.get("id"),
                "customer_type": cliente.get("type"),
                "customer_identification": cliente.get("identification"),
                "customer_name": customer_name,
                "customer_active": cliente.get("active"),
                "customer_vat_responsible": cliente.get("vat_responsible"),
                "customer_id_type_name": id_type.get("name"),
                "customer_fiscal_responsibility_name": fiscal_name,
                "customer_address": address_data.get("address"),
                "customer_city_name": city.get("city_name"),
                "customer_state_name": city.get("state_name"),
                "customer_phone_number": phone_number,
                "customer_email": cliente.get("email"),
            }
            all_data.append(row)

        page += 1

    logger.info(f"Extracción de clientes finalizada. Total registros: {len(all_data)}")
    return pd.DataFrame(all_data)

# Alias para compatibilidad
get_clientes = get_customers
