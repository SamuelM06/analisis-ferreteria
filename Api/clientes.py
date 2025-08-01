import requests
import pandas as pd
from datetime import date, timedelta
def get_clientes(access_token):
    Date_last = date.today() - timedelta(days=1)
    Looking_Type = 'created_end'
    reference_type = 'customers'
    api_url = f"https://api.siigo.com/v1/{reference_type}?{Looking_Type}={Date_last}"
    headers_api = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": "AnalisisFerreteria"
    }
    response_api = requests.get(api_url, headers=headers_api)
    flattened_data = []
    if response_api.status_code == 200:
        api = response_api.json()
        result_data = api['results']
        for cliente in result_data:
            row = {
                'customer_id': cliente.get('id'),
                'customer_name': cliente.get('name'),
                'customer_identification': cliente.get('identification'),
                'customer_phone': cliente.get('phone'),
                'customer_email': cliente.get('email'),
                'customer_type': cliente.get('type'),
                
            }
            flattened_data.append(row)
        df = pd.DataFrame(flattened_data)
        print(df)
        return df
    else:
        print("Error al obtener clientes:")
        print(response_api.status_code)
        print(response_api.text)
        return None