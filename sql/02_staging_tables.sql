-- ==============================================================================
-- 02. TABLAS DE STAGING (STG)
-- ==============================================================================
USE ferreteria_db;
GO

-- Tabla de Staging para Productos
IF OBJECT_ID('STG.products', 'U') IS NULL
BEGIN
    CREATE TABLE STG.products (
        product_id VARCHAR(50),
        product_code VARCHAR(50),
        product_name VARCHAR(150),
        product_price DECIMAL(18,2),
        product_cost DECIMAL(18,2),
        product_stock INT,
        product_status BIT,
        category_name VARCHAR(100),
        product_type VARCHAR(50),
        tax_classification VARCHAR(50),
        tax_included BIT,
        unit_label VARCHAR(50),
        brand VARCHAR(100),
        created_date DATETIME,
        last_updated DATETIME,
        has_stock_control BIT,
        warehouse_count INT
    );
END
GO

-- Tabla de Staging para Clientes
IF OBJECT_ID('STG.customers', 'U') IS NULL
BEGIN
    CREATE TABLE STG.customers (
        customer_id VARCHAR(50),
        customer_type VARCHAR(50),
        customer_identification VARCHAR(50),
        customer_name VARCHAR(200),
        customer_active BIT,
        customer_vat_responsible BIT,
        customer_id_type_name VARCHAR(50),
        customer_fiscal_responsibility_name VARCHAR(100),
        customer_address VARCHAR(255),
        customer_city_name VARCHAR(100),
        customer_state_name VARCHAR(100),
        customer_phone_number VARCHAR(50),
        customer_email VARCHAR(150)
    );
END
GO

-- Tabla de Staging para Facturas e Ítems
IF OBJECT_ID('STG.invoices', 'U') IS NULL
BEGIN
    CREATE TABLE STG.invoices (
        invoice_id VARCHAR(50) NULL,
        item_id VARCHAR(50) NULL,
        invoice_number VARCHAR(50),
        invoice_date DATETIME,
        status VARCHAR(50),
        created_date DATETIME,
        customer_id VARCHAR(50),
        customer_name VARCHAR(200),
        customer_email VARCHAR(200),
        customer_phone VARCHAR(50),
        seller_name VARCHAR(150),
        warehouse_name VARCHAR(150),
        product_id VARCHAR(50),
        item_description VARCHAR(MAX),
        item_quantity DECIMAL(18,6),
        item_price DECIMAL(18,2),
        item_total DECIMAL(18,2),
        item_tax DECIMAL(10,2),
        item_discount DECIMAL(10,2),
        reference_number VARCHAR(50),
        document_type VARCHAR(50),
        notes VARCHAR(MAX)
    );
END
GO
