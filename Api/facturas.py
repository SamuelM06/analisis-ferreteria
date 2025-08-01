import requests
import pandas as pd
from datetime import date, timedelta

def get_facturas(access_token):
    Date_last = date.today() - timedelta(days=1)
    Looking_Type = 'created_end'
    reference_type = 'invoices'
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
        for invoice in result_data:
            invoice_id = invoice.get('id')
            invoice_number = invoice.get('number')
            invoice_date = invoice.get('date')
            invoice_total = invoice.get('total')
            invoice_seller = invoice.get('seller')
            customer_data = invoice.get('customer', {})
            customer_id = customer_data.get('id')
            customer_identification = customer_data.get('identification')
            payments = invoice.get('payments', [])
            payment_method = payments[0]['name'] if payments else None
            payment_value = payments[0]['value'] if payments else None
            items = invoice.get('items', [])
            if items:
                for item in items:
                    row = {
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_number,
                        'invoice_date': invoice_date,
                        'invoice_total': invoice_total,
                        'seller_id': invoice_seller,
                        'customer_id': customer_id,
                        'customer_identification': customer_identification,
                        'payment_method': payment_method,
                        'payment_value': payment_value,
                        'item_id': item.get('id'),
                        'item_code': item.get('code'),
                        'item_quantity': item.get('quantity'),
                        'item_price': item.get('price'),
                        'item_description': item.get('description'),
                        'item_total': item.get('total')
                    }
                    flattened_data.append(row)
            else:
                row = {
                    'invoice_id': invoice_id,
                    'invoice_number': invoice_number,
                    'invoice_date': invoice_date,
                    'invoice_total': invoice_total,
                    'seller_id': invoice_seller,
                    'customer_id': customer_id,
                    'customer_identification': customer_identification,
                    'payment_method': payment_method,
                    'payment_value': payment_value,
                    'item_id': None,
                    'item_code': None,
                    'item_quantity': None,
                    'item_price': None,
                    'item_description': None,
                    'item_total': None
                }
                flattened_data.append(row)
        df = pd.DataFrame(flattened_data)
        print(df)
        return df
    else:
        print("Error al obtener api:")
        print(response_api.status_code)
        print(response_api.text)
        return None