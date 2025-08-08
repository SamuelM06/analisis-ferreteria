import requests
import pandas as pd
import time
def get_productos(access_token, max_retries=5, wait_time=60):
    """
    Trae los productos con los campos esenciales para el análisis.
    Controla error 429 y reintentos.
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
            print("Número máximo de reintentos alcanzado. Abortando descarga de productos.")
            return pd.DataFrame(all_data)
        api = response_api.json()
        result_data = api.get('results', [])
        if not result_data:
            break
        for producto in result_data:
            # Extraer precio y costo, que son objetos
            price_data = producto.get('price', [{}])
            product_price = price_data[0].get('value') if price_data else None
            product_cost = producto.get('cost')
            row = {
                'product_id': producto.get('id'),
                'product_code': producto.get('code'),
                'product_name': producto.get('name'),
                'product_price': product_price,
                'product_cost': product_cost,
                'product_stock': producto.get('stock'),
                'product_status': producto.get('status'),
            }
            all_data.append(row)
        page += 1
    df = pd.DataFrame(all_data)
    return df