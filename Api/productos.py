import requests
import pandas as pd

def get_productos(access_token):
    """
    Trae todos los productos, recorriendo todas las páginas de resultados.
    """
    reference_type = 'products'
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
        for producto in result_data:
            row = {
                'product_id': producto.get('id'),
                'product_code': producto.get('code'),
                'product_name': producto.get('name'),
                'product_reference': producto.get('reference'),
                'product_description': producto.get('description'),
                'product_price': producto.get('price'),
                'product_tax': producto.get('tax'),
                # Otros campos si necesitas
            }
            all_data.append(row)
        page += 1
    df = pd.DataFrame(all_data)
    return df