import os
from pathlib import Path
from dotenv import load_dotenv

# Localizar el directorio raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Cargar variables de entorno desde el archivo .env en la raíz
ENV_FILE = BASE_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)
else:
    load_dotenv()

class Settings:
    """Configuraciones del sistema cargadas desde variables de entorno."""

    # Siigo API
    SIIGO_USERNAME: str = os.getenv("SIIGO_USERNAME", "").strip()
    SIIGO_ACCESS_KEY: str = os.getenv("SIIGO_ACCESS_KEY", "").strip()
    SIIGO_PARTNER_ID: str = os.getenv("SIIGO_PARTNER_ID", "AnalisisFerreteria").strip()
    SIIGO_AUTH_URL: str = os.getenv("SIIGO_AUTH_URL", "https://api.siigo.com/auth").strip()
    SIIGO_API_BASE_URL: str = os.getenv("SIIGO_API_BASE_URL", "https://api.siigo.com/v1").strip()

    # SQL Server
    DB_DRIVER: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server").strip()
    DB_SERVER: str = os.getenv("DB_SERVER", "localhost").strip()
    DB_NAME: str = os.getenv("DB_NAME", "ferreteria_db").strip()
    DB_TRUSTED_CONNECTION: str = os.getenv("DB_TRUSTED_CONNECTION", "yes").strip()
    DB_ENCRYPT: str = os.getenv("DB_ENCRYPT", "no").strip()
    DB_TRUST_SERVER_CERTIFICATE: str = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "yes").strip()
    DB_USER: str = os.getenv("DB_USER", "").strip()
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "").strip()

    # Logging & Rutas
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").strip()
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/etl_ferreteria.log").strip()
    BASE_DIR: Path = BASE_DIR

    @property
    def connection_string(self) -> str:
        """Genera la cadena de conexión ODBC según el modo configurado."""
        params = [
            f"DRIVER={{{self.DB_DRIVER}}}",
            f"SERVER={self.DB_SERVER}",
            f"DATABASE={self.DB_NAME}",
            f"Encrypt={self.DB_ENCRYPT}",
            f"TrustServerCertificate={self.DB_TRUST_SERVER_CERTIFICATE}",
        ]

        if self.DB_TRUSTED_CONNECTION.lower() in ("yes", "true", "1"):
            params.append("Trusted_Connection=yes")
        elif self.DB_USER and self.DB_PASSWORD:
            params.append(f"UID={self.DB_USER}")
            params.append(f"PWD={self.DB_PASSWORD}")
        else:
            params.append("Trusted_Connection=yes")

        return ";".join(params) + ";"

    def validate(self) -> None:
        """Valida que las credenciales mínimas estén definidas."""
        missing = []
        if not self.SIIGO_USERNAME or "ejemplo.com" in self.SIIGO_USERNAME:
            missing.append("SIIGO_USERNAME")
        if not self.SIIGO_ACCESS_KEY or "tu_access_key" in self.SIIGO_ACCESS_KEY:
            missing.append("SIIGO_ACCESS_KEY")
        if not self.DB_SERVER:
            missing.append("DB_SERVER")

        if missing:
            raise ValueError(
                f"Faltan variables de entorno requeridas en el archivo .env: {', '.join(missing)}.\n"
                f"Por favor revisa el archivo .env o consulta .env.example."
            )

settings = Settings()
