import requests
import pandas as pd
import time
def get_clientes(access_token, max_retries=5, wait_time=60):
    """
    Extrae los clientes con los campos esenciales para el análisis.
    Controla error 429 y reintentos.
    """
    reference_type = 'customers'
    base_url = f"https://api.siigo.com/v1/{reference_type}"
    headers_api = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": "AnalisisFerreteria"
    }
    all_data = []
    page = 1
    while True:
        api_url = f"{base_url}?page={page}"
        retries = 0
        while retries <= max_retries:
            response_api = requests.get(api_url, headers=headers_api)
            if response_api.status_code == 200:
                break
            elif response_api.status_code == 429:
                print(f"Error 429 en página {page}. Esperando {wait_time} segundos antes de reintentar...")
                time.sleep(wait_time)
                retries += 1
            else:
                print(f"Error en página {page}: {response_api.status_code}")
                return pd.DataFrame(all_data)
        if retries > max_retries:
            print("Número máximo de reintentos alcanzado. Abortando descarga de clientes.")
            return pd.DataFrame(all_data)
        api = response_api.json()
        result_data = api.get('results', [])
        if not result_data:
            break
        for cliente in result_data:
            # Extracción robusta de datos, manejando nulos y listas vacías
            id_type = cliente.get('id_type', {})
            city = cliente.get('city', {})
            address_data = cliente.get('address', {})
            phone_data = cliente.get('phones', [])
            fiscal_resp = cliente.get('fiscal_responsibilities', [])
            
            # Lógica de extracción segura para evitar el error "sequence item"
            customer_name_data = cliente.get('name')
            if isinstance(customer_name_data, list):
                customer_name = " ".join(str(x) for x in customer_name_data if x is not None)
            else:
                customer_name = str(customer_name_data) if customer_name_data is not None else ""
            
            customer_phone_number = phone_data[0].get('number') if phone_data and isinstance(phone_data, list) and phone_data[0] else None
            
            fiscal_responsibility_name = fiscal_resp[0].get('name') if fiscal_resp and isinstance(fiscal_resp, list) and fiscal_resp[0] else None
            
            row = {
                'customer_id': cliente.get('id'),
                'customer_type': cliente.get('type'),
                'customer_identification': cliente.get('identification'),
                'customer_name': customer_name,
                'customer_active': cliente.get('active'),
                'customer_vat_responsible': cliente.get('vat_responsible'),
                'customer_id_type_name': id_type.get('name'),
                'customer_fiscal_responsibility_name': fiscal_responsibility_name,
                'customer_address': address_data.get('address'),
                'customer_city_name': city.get('city_name'),
                'customer_state_name': city.get('state_name'),
                'customer_phone_number': customer_phone_number,
                'customer_email': cliente.get('email'),
            }
            all_data.append(row)
        page += 1
    df = pd.DataFrame(all_data)
    return df