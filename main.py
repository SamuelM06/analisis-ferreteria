#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pipeline Principal ETL - Análisis de Datos Ferretería
Orquestador de extracción de Siigo API y carga en SQL Server.
"""

import sys
import time
from config.settings import settings
from src.utils.logger import get_logger
from src.database.connection import test_connection
from src.database.repository import insert_customers, insert_products, insert_invoices
from src.extractors.auth import get_siigo_token
from src.extractors.customers import get_customers
from src.extractors.products import get_products
from src.extractors.invoices import get_invoices

logger = get_logger("ETL_Main")

def run_pipeline():
    """Ejecuta el pipeline completo de extracción y carga."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE ETL - ANÁLISIS FERRETERÍA")
    logger.info("=" * 60)

    # 1. Validar configuración y credenciales
    try:
        settings.validate()
        logger.info("Variables de entorno y credenciales validadas correctamente.")
    except ValueError as e:
        logger.critical(f"Error de configuración: {e}")
        sys.exit(1)

    # 2. Probar conectividad con base de datos
    logger.info(f"Verificando conexión con SQL Server ({settings.DB_SERVER} / {settings.DB_NAME})...")
    if not test_connection():
        logger.critical("No se pudo establecer conexión con SQL Server. Deteniendo ejecución.")
        sys.exit(1)

    # 3. Autenticación con Siigo API
    try:
        access_token = get_siigo_token()
    except Exception as e:
        logger.critical(f"Fallo crítico en autenticación con Siigo: {e}")
        sys.exit(1)

    # Métricas de resumen
    resumen = {
        "clientes_extraidos": 0,
        "clientes_insertados": 0,
        "productos_extraidos": 0,
        "productos_insertados": 0,
        "facturas_extraidas": 0,
        "facturas_insertadas": 0,
    }

    # 4. Fase de Clientes
    logger.info("-" * 50)
    logger.info("FASE 1: CLIENTES")
    logger.info("-" * 50)
    try:
        df_clientes = get_customers(access_token)
        resumen["clientes_extraidos"] = len(df_clientes)
        resumen["clientes_insertados"] = insert_customers(df_clientes)
    except Exception as e:
        logger.error(f"Error en fase de clientes: {e}")

    # 5. Fase de Productos
    logger.info("-" * 50)
    logger.info("FASE 2: PRODUCTOS")
    logger.info("-" * 50)
    try:
        df_productos = get_products(access_token)
        resumen["productos_extraidos"] = len(df_productos)
        resumen["productos_insertados"] = insert_products(df_productos)
    except Exception as e:
        logger.error(f"Error en fase de productos: {e}")

    # 6. Fase de Facturas
    logger.info("-" * 50)
    logger.info("FASE 3: FACTURAS")
    logger.info("-" * 50)
    try:
        df_facturas = get_invoices(access_token)
        resumen["facturas_extraidas"] = len(df_facturas)
        resumen["facturas_insertadas"] = insert_invoices(df_facturas)
    except Exception as e:
        logger.error(f"Error en fase de facturas: {e}")

    # Resumen final
    elapsed = round(time.time() - start_time, 2)
    logger.info("=" * 60)
    logger.info("RESUMEN DE EJECUCIÓN ETL")
    logger.info(f"Clientes:  {resumen['clientes_extraidos']} extraídos | {resumen['clientes_insertados']} nuevos insertados")
    logger.info(f"Productos: {resumen['productos_extraidos']} extraídos | {resumen['productos_insertados']} nuevos insertados")
    logger.info(f"Facturas:  {resumen['facturas_extraidas']} extraídas | {resumen['facturas_insertadas']} registros insertados")
    logger.info(f"Tiempo total: {elapsed} segundos")
    logger.info("=" * 60)
    logger.info("¡PROCESO COMPLETADO EXITOSAMENTE!")

if __name__ == "__main__":
    run_pipeline()
