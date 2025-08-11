DROP DATABASE ferreteria_db;
use TestDB_Python;

-- Crear la base de datos
CREATE DATABASE ferreteria_db;
GO

-- Usar la base de datos
USE ferreteria_db;
GO

-- Crear el esquema STG
CREATE SCHEMA STG;
GO

-- Creación de la tabla STG.products con todos los campos traídos desde la API
CREATE TABLE STG.products (
    product_id VARCHAR(50),               -- id
    product_code VARCHAR(50),             -- code
    product_name VARCHAR(100),            -- name
    product_price DECIMAL(18,2),          -- price (primer valor)
    product_cost DECIMAL(18,2),           -- cost
    product_stock INT,                    -- stock
    product_status VARCHAR(50)            -- status
);
GO

-- Creación tabla STG.customers con todos los campos extraídos de la API Siigo
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
    customer_phone_number VARCHAR(20),
    customer_email VARCHAR(150)
);
GO

-- Tabla STG.invoices (sin cambios ya que está completa)
CREATE TABLE STG.invoices (
    invoice_id VARCHAR(50),
    invoice_number VARCHAR(50),
    invoice_date DATE,
    invoice_total DECIMAL(10, 2),
    seller_id VARCHAR(50),
    customer_id VARCHAR(50),
    customer_identification VARCHAR(50),
    payment_method VARCHAR(50),
    payment_value DECIMAL(10, 2),
    item_id VARCHAR(50),
    item_code VARCHAR(50),
    item_quantity DECIMAL(18,6),
    item_price DECIMAL(10, 2),
    item_description VARCHAR(255),
    item_total DECIMAL(10, 2)
);
GO

Select * from STG.customers;
Select * from STG.invoices;
Select * from STG.products;


ALTER TABLE STG.invoices
ALTER COLUMN item_quantity DECIMAL(18,6);

SELECT * FROM STG.invoices
ORDER BY invoice_date ASC;

-- Limpia la tabla productos
TRUNCATE TABLE STG.products;

ALTER TABLE STG.products
ADD category_name VARCHAR(100),  -- Nombre categoría
    product_type  VARCHAR(50);   -- Tipo de producto
GO
ALTER TABLE STG.products
ADD tax_classification VARCHAR(50),  -- Excluded, Exempt, Taxed
    tax_included BIT,                -- 1 si el precio incluye IVA, 0 si no
    unit_label VARCHAR(50),          -- Unidad de medida escrita (ej: unidad, metro, caja)
    brand VARCHAR(100),              -- Marca del producto
    created_date DATETIME,           -- Fecha de creación en Siigo
    last_updated DATETIME,           -- Última actualización en Siigo
    has_stock_control BIT,           -- 1 si Siigo controla stock, 0 si no
    warehouse_count INT;              -- Número de bodegas en las que está el producto
GO
