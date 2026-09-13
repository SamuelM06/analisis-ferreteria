import logging
import sys
from pathlib import Path
from config.settings import settings

_loggers = {}

def get_logger(name: str = "ETL_Pipeline") -> logging.Logger:
    """
    Retorna una instancia configurada de logger para registrar en consola y archivo.
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    level_name = settings.LOG_LEVEL.upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    # Evitar duplicación de handlers si ya existen
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Consola (stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Archivo de logs en logs/
        try:
            log_path = Path(settings.LOG_FILE)
            if not log_path.is_absolute():
                log_path = settings.BASE_DIR / log_path

            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"No se pudo inicializar el archivo de logs: {e}")

    _loggers[name] = logger
    return logger
