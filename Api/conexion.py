import os
import requests
import json

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
api_url = "https://api.siigo.com/v1/customers"

# Headers para la consulta (aquí agregamos Partner-Id)
headers_api = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Partner-Id": "AnalisisFerreteria"
}

# Solicitud GET de api
response_api = requests.get(api_url, headers=headers_api)
if response_api.status_code == 200:
    api = response_api.json()
    print(json.dumps(api, indent=4, ensure_ascii=False))
else:
    print("Error al obtener api:")
    print(response_api.status_code)
    print(response_api.text)