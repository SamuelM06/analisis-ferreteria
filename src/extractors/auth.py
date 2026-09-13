import requests
from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("SiigoAuth")

def get_siigo_token() -> str:
    """
    Solicita y retorna un Bearer Token desde la API de autenticación de Siigo.
    Utiliza las credenciales definidas de forma segura en las variables de entorno (.env).
    """
    settings.validate()

    body = {
        "username": settings.SIIGO_USERNAME,
        "access_key": settings.SIIGO_ACCESS_KEY
    }
    headers = {
        "Content-Type": "application/json"
    }

    logger.info(f"Autenticando en Siigo API con el usuario: {settings.SIIGO_USERNAME[:3]}***@***...")

    try:
        response = requests.post(settings.SIIGO_AUTH_URL, json=body, headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error(f"Error de red al conectar con el servidor de autenticación de Siigo: {e}")
        raise

    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("La respuesta de Siigo no contuvo un 'access_token' válido.")
        logger.info("Token de autenticación obtenido exitosamente.")
        return access_token
    else:
        logger.error(f"Fallo de autenticación Siigo ({response.status_code}): {response.text}")
        raise RuntimeError(f"Error al autenticar en Siigo API. Código: {response.status_code}")
