import os

import requests

import json

from datetime import date, timedelta
import pandas as pd

os.system("cls")



# DATOS DE ACCESO

usuario = "shirleymariadiazherrera@gmail.com"  

access_key = "ZDE4ODEzOTItZDQ3NC00NTNhLTg3NDYtNzRjOTI4NGQ4ZmJiOk5IaG0oNXsjL0E="        



# URL DEL ENDPOINT DE AUTENTICACION

url_auth = "https://api.siigo.com/auth"



#  body de autenticación

body_auth = {

    "username": usuario,

    "access_key": access_key

}



# Headers para autenticación

headers_auth = {

    "Content-Type": "application/json"

}



# PETICION POST PARA GENERAR TOKEN

response = requests.post(url_auth, headers=headers_auth, data=json.dumps(body_auth))



# Manejo de la respuesta (creación de token)

if response.status_code == 200:

    token_data = response.json()

    access_token = token_data["access_token"]    # Guardar el token de acceso

    print("TOKEN obtenido correctamente.")

else:

    print(f"Error de autenticación: {response.status_code}")

    print(response.text)

    exit(1)



# URL del endpoint de api

# Resta un día usando timedelta
Date_last = date.today() - timedelta(days=1)
Looking_Type='created_end'
reference_type='invoices'


api_url = f"https://api.siigo.com/v1/{reference_type}?{Looking_Type}={Date_last}"



# Headers para la consulta (aquí agregamos Partner-Id)

headers_api = {

    "Authorization": f"Bearer {access_token}",

    "Content-Type": "application/json",

    "Partner-Id": "AnalisisFerreteria"

}



# Solicitud GET de api
flattened_data=[]

response_api = requests.get(api_url, headers=headers_api)

if response_api.status_code == 200:

    api = response_api.json()

    #print(json.dumps(api, indent=4, ensure_ascii=False))

    result_data = api['results']
    pagination=api['pagination']
    print("Accediendo al atributo 'result':")
    
    
    #for invoice in result_data:
    #   print(invoice.get('description'))



    for invoice in result_data:
    # Extract common invoice-level data
        invoice_id = invoice.get('id')
        invoice_number = invoice.get('number')
        invoice_date = invoice.get('date')
        invoice_total = invoice.get('total')
        invoice_seller = invoice.get('seller')

        # Extract nested customer data
        customer_data = invoice.get('customer', {})
        customer_id = customer_data.get('id')
        customer_identification = customer_data.get('identification')
        
        # Extract nested payments data
        payments = invoice.get('payments', [])
        payment_method = payments[0]['name'] if payments else None
        payment_value = payments[0]['value'] if payments else None

        # Check if there are items in the invoice
        items = invoice.get('items', [])
        if items:
            # Iterate over each item in the current invoice
            for item in items:
                # Create a dictionary for the flattened row
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
            # If no items, create a row with NaN values for item-specific fields
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

# Convert the list of flattened dictionaries to a DataFrame
    df = pd.DataFrame(flattened_data)
    print(df)

else:

    print("Error al obtener api:")

    print(response_api.status_code)

    print(response_api.text)