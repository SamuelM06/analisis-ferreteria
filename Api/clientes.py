import requests
import pandas as pd

def get_clientes(access_token):
    """
    Extrae todos los clientes, recorriendo todas las páginas.
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
        response_api = requests.get(api_url, headers=headers_api)
        if response_api.status_code != 200:
            print(f"Error en página {page}: {response_api.status_code}")
            break
        api = response_api.json()
        result_data = api.get('results', [])
        if not result_data:
            break
        for cliente in result_data:
            row = {
                'customer_id': cliente.get('id'),
                'customer_name': cliente.get('name'),
                'customer_identification': cliente.get('identification'),
                'customer_phone': cliente.get('phone'),
                'customer_email': cliente.get('email'),
                'customer_type': cliente.get('type'),
                # Otros campos si necesitas
            }
            all_data.append(row)
        page += 1
    df = pd.DataFrame(all_data)
    return df