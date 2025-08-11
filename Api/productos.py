import requests
import pandas as pd
import time
import math

def get_productos(access_token, max_retries=5, wait_time=60):
    """
    Descarga todos los productos de Siigo con datos extendidos.
    Maneja reintentos ante 429 y devuelve un DataFrame listo para insertar.
    """
    base_url = "https://api.siigo.com/v1"
    headers_api = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Partner-Id": "AnalisisFerreteria"
    }

    all_data = []
    page = 1

    while True:
        api_url = f"{base_url}/products?page={page}"
        retries = 0

        while retries <= max_retries:
            response_api = requests.get(api_url, headers=headers_api)
            if response_api.status_code == 200:
                break
            elif response_api.status_code == 429:
                print(f"[API] Error 429 en página {page}. Esperando {wait_time} segundos...")
                time.sleep(wait_time)
                retries += 1
            else:
                print(f"[API] Error {response_api.status_code} en página {page}")
                return pd.DataFrame(all_data)

        if retries > max_retries:
            print("[API] Número máximo de reintentos alcanzado.")
            return pd.DataFrame(all_data)

        api_response = response_api.json()
        result_data = api_response.get('results', [])
        if not result_data:
            break

        for producto in result_data:
            try:
                # Básicos
                product_id = producto.get('id')
                product_code = producto.get('code')
                product_name = producto.get('name')
                product_status = producto.get('active')

                # Categoría y tipo
                category_name = None
                account_group = producto.get('account_group', {})
                if account_group and isinstance(account_group, dict):
                    category_name = account_group.get('name')
                product_type = producto.get('type')

                # Precio
                product_price = 0.0
                prices = producto.get('prices', [])
                if prices and isinstance(prices, list):
                    first_price_list = prices[0].get('price_list', [])
                    if first_price_list and isinstance(first_price_list, list):
                        val = first_price_list[0].get('value')
                        if val is not None and not isinstance(val, str):
                            product_price = float(val)

                # Stock
                stock_val = producto.get('available_quantity', 0.0)
                if stock_val is None or isinstance(stock_val, str) or math.isnan(stock_val):
                    product_stock = 0.0
                else:
                    product_stock = float(stock_val)

                # Campos extra
                tax_classification = producto.get('tax_classification')
                tax_included = producto.get('tax_included')
                unit_label = producto.get('unit_label') or (
                    producto.get('unit', {}).get('name') if producto.get('unit') else None
                )
                brand = producto.get('additional_fields', {}).get('brand')
                created_date = producto.get('metadata', {}).get('created')
                last_updated = producto.get('metadata', {}).get('last_updated')
                has_stock_control = producto.get('stock_control')
                warehouse_count = len(producto.get('warehouses', []))

                all_data.append({
                    'product_id': product_id,
                    'product_code': product_code,
                    'product_name': product_name,
                    'product_price': product_price,
                    'product_stock': product_stock,
                    'product_status': product_status,
                    'category_name': category_name,
                    'product_type': product_type,
                    'tax_classification': tax_classification,
                    'tax_included': tax_included,
                    'unit_label': unit_label,
                    'brand': brand,
                    'created_date': created_date,
                    'last_updated': last_updated,
                    'has_stock_control': has_stock_control,
                    'warehouse_count': warehouse_count
                })

            except Exception as e:
                print(f"[ERROR] Procesando producto {producto.get('code')}: {e}")

        page += 1

    df = pd.DataFrame(all_data)
    return df