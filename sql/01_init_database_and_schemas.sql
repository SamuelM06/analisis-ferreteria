-- ==============================================================================
-- 01. INICIALIZACIÓN DE BASE DE DATOS Y ESQUEMAS
-- ==============================================================================
-- Crea la base de datos ferreteria_db y los esquemas STG, DIM y FACT.
-- ==============================================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ferreteria_db')
BEGIN
    CREATE DATABASE ferreteria_db;
END
GO

USE ferreteria_db;
GO

-- Esquema de Staging (área de aterrizaje para extracción API)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'STG')
BEGIN
    EXEC('CREATE SCHEMA STG;');
END
GO

-- Esquema de Dimensiones (Modelo Estrella)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'DIM')
BEGIN
    EXEC('CREATE SCHEMA DIM;');
END
GO

-- Esquema de Hechos / Métricas de Negocio
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'FACT')
BEGIN
    EXEC('CREATE SCHEMA FACT;');
END
GO
