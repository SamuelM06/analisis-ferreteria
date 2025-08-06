-- Crear la base de datos
CREATE DATABASE ferreteria_db;
GO

-- Usar la base de datos
USE ferreteria_db;
GO

-- Crear el esquema STG
CREATE SCHEMA STG;
GO

-- Crear la tabla STG.products
CREATE TABLE STG.products (
    product_id VARCHAR(50),
    product_code VARCHAR(50),
    product_name VARCHAR(100),
    product_reference VARCHAR(50),
    product_description VARCHAR(255),
    product_price DECIMAL(10, 2),
    product_tax DECIMAL(5, 2)
);
GO

-- Crear la tabla STG.customers
CREATE TABLE STG.customers (
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    customer_identification VARCHAR(50),
    customer_phone VARCHAR(20),
    customer_email VARCHAR(100),
    customer_type VARCHAR(50)
);
GO

-- Crear la tabla STG.invoices
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
    item_quantity INT,
    item_price DECIMAL(10, 2),
    item_description VARCHAR(255),
    item_total DECIMAL(10, 2)
);
GO

Select * from STG.customers;
Select * from STG.invoices;
Select * from STG.products;


