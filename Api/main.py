import json
import requests
import os
import pandas as pd

from conexion import insert_customers, insert_products, insert_invoices

os.system("cls")  

# Datos de acceso
usuario = "shirleymariadiazherrera@gmail.com"
access_key = "ZDE4ODEzOTItZDQ3NC00NTNhLTg3NDYtNzRjOTI4NGQ4ZmJiOk5IaG0oNXsjL0E="

def get_token():
    url_auth = "https://api.siigo.com/auth"
    body_auth = {
        "username": usuario,
        "access_key": access_key
    }
    headers_auth = {
        "Content-Type": "application/json"
    }
    response = requests.post(url_auth, headers=headers_auth, data=json.dumps(body_auth))
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data["access_token"]
        print("TOKEN obtenido correctamente.")
        print("Cargando los datos.....")
        return access_token
    else:
        print(f"Error de autenticación: {response.status_code}")
        print(response.text)
        exit(1)

if __name__ == '__main__':
    access_token = get_token()

    # --- CLIENTES ---
    try:
        import clientes
        print("Obteniendo clientes...")
        df_clientes = clientes.get_clientes(access_token)

        # Solución: convertir listas a string para customer_name
        df_clientes["customer_name"] = df_clientes["customer_name"].apply(
            lambda x: " ".join(x) if isinstance(x, list) else x
        )
        df_clientes = df_clientes.where(pd.notnull(df_clientes), None)   # Evita NaN
        insert_customers(df_clientes)
    except Exception as e:
        print(f"Error con clientes: {e}")

    # --- PRODUCTOS ---
    try:
        import productos
        print("Obteniendo productos...")
        df_productos = productos.get_productos(access_token)
        df_productos = df_productos.where(pd.notnull(df_productos), None)
        insert_products(df_productos)
    except Exception as e:
        print(f"Error con productos: {e}")

    # --- FACTURAS ---
    try:
        import facturas
        print("Obteniendo facturas...")
        df_facturas = facturas.get_facturas(access_token)
        df_facturas = df_facturas.where(pd.notnull(df_facturas), None)
        insert_invoices(df_facturas)
    except Exception as e:
        print(f"Error con facturas: {e}")

    print("\n¡PROCESO COMPLETADO! Puedes consultar ahora en SQL Server.")