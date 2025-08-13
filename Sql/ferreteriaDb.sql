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


CREATE TABLE STG.invoices (
    -- Claves principales
    invoice_id        VARCHAR(50)     NOT NULL,  -- ID de la factura en Siigo
    item_id           VARCHAR(50)     NOT NULL,  -- ID del ítem (línea de factura)

    -- Datos de la factura
    invoice_number    VARCHAR(50),              -- Número de factura
    invoice_date      DATETIME,                 -- Fecha de emisión
    status            VARCHAR(50),              -- Estado de la factura (ej: Paid, Draft, Cancelled)
    created_date      DATETIME,                 -- Fecha de creación en Siigo
    last_updated      DATETIME,                 -- Fecha de última modificación en Siigo

    -- Datos del cliente
    customer_id       VARCHAR(50),              -- ID del cliente en Siigo
    customer_name     VARCHAR(200),             -- Nombre del cliente
    customer_email    VARCHAR(200),             -- Correo electrónico
    customer_phone    VARCHAR(50),              -- Teléfono

    -- Datos del vendedor y bodega
    seller_name       VARCHAR(150),             -- Nombre del vendedor
    warehouse_name    VARCHAR(150),             -- Nombre de la bodega

    -- Detalle del ítem
    product_id        VARCHAR(50),              -- ID del producto
    item_description  VARCHAR(MAX),             -- Descripción del ítem
    item_quantity     DECIMAL(18,6),             -- Cantidad
    item_price        DECIMAL(18,2),             -- Precio unitario
    item_total        DECIMAL(18,2),             -- Total línea
    item_tax          DECIMAL(10,2),             -- Impuesto aplicado
    item_discount     DECIMAL(10,2),             -- Descuento aplicado

    -- Información financiera extra
    currency          VARCHAR(10),              -- Moneda
    payment_method    VARCHAR(50),              -- Forma de pago
    due_date          DATETIME,                 -- Fecha de vencimiento
    reference_number  VARCHAR(50),              -- Número de referencia
    document_type     VARCHAR(50),              -- Tipo de documento

    -- Observaciones
    notes             VARCHAR(MAX),             -- Notas de la factura
);
GO
TRUNCATE TABLE STG.invoices;
SELECT * FROM STG.invoices;