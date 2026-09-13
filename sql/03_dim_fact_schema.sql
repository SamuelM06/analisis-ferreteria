-- ==============================================================================
-- 03. MODELO DIMENSIONAL (DATA WAREHOUSE)
-- ==============================================================================
USE ferreteria_db;
GO

-- Dimensión Productos
IF OBJECT_ID('DIM.DIM_PRODUCTOS', 'U') IS NULL
BEGIN
    CREATE TABLE DIM.DIM_PRODUCTOS (
        product_id INT IDENTITY (1,1) PRIMARY KEY,
        product_id_native VARCHAR(50),
        product_code VARCHAR(50),
        product_name VARCHAR(150),
        product_price DECIMAL(18,2),
        product_stock INT,
        product_status VARCHAR(50),
        category_name VARCHAR(100),
        product_type VARCHAR(50),
        fecha_carga DATETIME DEFAULT GETDATE()
    );
END
GO

-- Dimensión Clientes
IF OBJECT_ID('DIM.DIM_CLIENTE', 'U') IS NULL
BEGIN
    CREATE TABLE DIM.DIM_CLIENTE (
        customer_id INT IDENTITY (1,1) PRIMARY KEY,
        customer_id_native VARCHAR(50),
        customer_name VARCHAR(200),
        customer_identification VARCHAR(50),
        customer_city VARCHAR(100),
        customer_state VARCHAR(100),
        fecha_carga DATETIME DEFAULT GETDATE()
    );
END
GO

-- Tabla de Hechos: Facturas
IF OBJECT_ID('FACT.FACTURAS', 'U') IS NULL
BEGIN
    CREATE TABLE FACT.FACTURAS (
        factura_key INT IDENTITY(1,1) PRIMARY KEY,
        numero_factura NVARCHAR(50) NOT NULL,
        fecha_factura DATE NOT NULL,
        id_cliente_dw INT NOT NULL,
        id_producto_dw INT NOT NULL,
        cantidad INT NOT NULL,
        total DECIMAL(18, 2) NOT NULL,
        descuento DECIMAL(18, 2) NOT NULL,
        fecha_carga DATE NOT NULL
    );
END
GO
