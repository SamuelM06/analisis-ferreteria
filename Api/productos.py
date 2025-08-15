import requests
import pandas as pd
import time

def safe_get(data, *keys):
    """Extrae un valor anidado de forma segura, manejando diccionarios y listas."""
    for key in keys:
        if data is None:
            return None
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and isinstance(key, int):
            try:
                data = data[key]
            except IndexError:
                return None
        else:
            return None
    return data

def get_productos(access_token, max_retries=5, wait_time=60):
    """
    Descarga todos los productos de Siigo con datos extendidos.
    Maneja reintentos ante 429 y devuelve un DataFrame listo.
    """
    print("[API] 📦 Descargando productos...")
    base_url = "https://api.siigo.com/v1/products"
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
            response = requests.get(api_url, headers=headers_api)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                print(f"[API] Error 429 en página {page}. Esperando {wait_time} segundos...")
                time.sleep(wait_time)
                retries += 1
            else:
                print(f"[API] Error {response.status_code} en página {page}")
                return pd.DataFrame(all_data)

        if retries > max_retries:
            print("[API] Número máximo de reintentos alcanzado.")
            return pd.DataFrame(all_data)

        api_response = response.json()
        result_data = api_response.get('results', [])
        if not result_data:
            break

        for producto in result_data:
            product_price = safe_get(producto, 'prices', 0, 'price_list', 0, 'value')
            product_stock = safe_get(producto, 'available_quantity')
            product_cost = None
            brand = safe_get(producto, 'additional_fields', 'brand')
            unit_label = safe_get(producto, 'unit', 'unit_label')

            row = {
                'product_id': producto.get('id'),
                'product_code': producto.get('code', ''),
                'product_name': producto.get('name', ''),
                'product_price': product_price,
                'product_cost': product_cost,
                'product_stock': product_stock,
                'product_status': producto.get('active'),
                'category_name': safe_get(producto, 'account_group', 'name'),
                'product_type': safe_get(producto, 'type'),
                'tax_classification': safe_get(producto, 'tax_classification'),
                'tax_included': safe_get(producto, 'tax_included'),
                'unit_label': unit_label,
                'brand': brand,
                'created_date': safe_get(producto, 'metadata', 'created'),
                'last_updated': safe_get(producto, 'metadata', 'last_updated'),
                'has_stock_control': producto.get('stock_control'),
                'warehouse_count': len(producto.get('warehouses', []))
            }
            all_data.append(row)
        
        print(f"[API] Página {page} descargada. Total acumulado: {len(all_data)} registros.")
        page += 1

    print(f"[DEBUG] Productos extraídos: {len(all_data)} registros.")
    
    # <--- CORRECCIÓN CLAVE: Definir el orden de las columnas del DataFrame
    columns_order = [
        'product_id', 'product_code', 'product_name', 'product_price',
        'product_cost', 'product_stock', 'product_status', 'category_name',
        'product_type', 'tax_classification', 'tax_included', 'unit_label',
        'brand', 'created_date', 'last_updated', 'has_stock_control',
        'warehouse_count'
    ]
    return pd.DataFrame(all_data, columns=columns_order)