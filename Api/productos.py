import requests
import pandas as pd

def get_productos(access_token):
    api_url = "https://api.siigo.com/v1/products"
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
        for producto in result_data:
            row = {
                'product_id': producto.get('id'),
                'product_code': producto.get('code'),
                'product_name': producto.get('name'),
                'product_reference': producto.get('reference'),
                'product_description': producto.get('description'),
                'product_price': producto.get('price'),
                'product_tax': producto.get('tax'),
                # Agrega aquí otros campos que necesites
            }
            flattened_data.append(row)
        df = pd.DataFrame(flattened_data)
        print(df)
        return df
    else:
        print("Error al obtener productos:")
        print(response_api.status_code)
        print(response_api.text)
        return None