import json
import requests
import os
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
    # nombrar a los otros scripts aquí 
    #import facturas
    #df_facturas = facturas.get_facturas(access_token)
    
    import clientes
    df_clientes = clientes.get_clientes(access_token)
    
    #import productos
    #df_productos = productos.get_productos(access_token)