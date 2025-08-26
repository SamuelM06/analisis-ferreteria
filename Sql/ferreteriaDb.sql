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

-- Creaci�n de la tabla STG.products con todos los campos tra�dos desde la API
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

-- Creaci�n tabla STG.customers con todos los campos extra�dos de la API Siigo
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
    -- Claves
    invoice_id       VARCHAR(50) NULL,           -- puede venir vac�o; generamos uno si no
    item_id          VARCHAR(50) NULL,

    -- Datos de la factura
    invoice_number   VARCHAR(50),
    invoice_date     DATETIME,
    status           VARCHAR(50),
    created_date     DATETIME,

    -- Datos del cliente (solo si el cliente est� creado en Siigo)
    customer_id      VARCHAR(50),
    customer_name    VARCHAR(200),
    customer_email   VARCHAR(200),
    customer_phone   VARCHAR(50),

    -- Vendedor/Bodega
    seller_name      VARCHAR(150),
    warehouse_name   VARCHAR(150),

    -- �tems
    product_id       VARCHAR(50),
    item_description VARCHAR(MAX),
    item_quantity    DECIMAL(18,6),
    item_price       DECIMAL(18,2),
    item_total       DECIMAL(18,2),
    item_tax         DECIMAL(10,2),
    item_discount    DECIMAL(10,2),

    -- Otros visibles en el PDF
    reference_number VARCHAR(50),
    document_type    VARCHAR(50),
    notes            VARCHAR(MAX)
);
GO


ALTER TABLE STG.invoices
ALTER COLUMN invoice_id UNIQUEIDENTIFIER NULL;



TRUNCATE TABLE STG.products;
SELECT * FROM STG.invoices where created_date= '2025-08-13';
select * from STG.customers where customer_name like 'r%';
select * from STG.products;
use ferreteria_db;
SELECT 
*
FROM (
		SELECT ROW_NUMBER() OVER( PARTITION BY product_code ORDER BY 
		product_code) AS ROW_ID,*  FROM [STG].[products]
	)F WHERE F.ROW_ID =1


CREATE TABLE DIM.[DIM_PRODUCTOS](
    product_id INT IDENTITY (1,1),
    product_id_native VARCHAR(50),               
    product_code VARCHAR(50),             
    product_name VARCHAR(100),            
    product_price DECIMAL(18,2),          
    product_stock INT,                      
    product_status VARCHAR(50),
    category_name VARCHAR(50),
    product_type VARCHAR (50),
    fecha_carga DATETIME DEFAULT GETDATE()
 );


 select * from [STG].[invoices];
 select * from [DIM].[DIM_CLIENTE];
 select * from [DIM].[DIM_PRODUCTOS];



-- Crea la tabla FACT.FACTURAS
CREATE TABLE FACT.FACTURAS (
    numero_factura      NVARCHAR(50)  NOT NULL, -- O INT, si solo contiene n�meros
    fecha_factura       DATE          NOT NULL,
    id_cliente_dw       INT           NOT NULL, -- Ajusta el tipo de dato seg�n tu DIM_CLIENTE
    id_producto_dw      INT           NOT NULL, -- Ajusta el tipo de dato seg�n tu DIM_PRODUCTOS
    cantidad            INT           NOT NULL,
    total               DECIMAL(18, 2)NOT NULL,
    descuento           DECIMAL(18, 2)NOT NULL,
    Fecha_carga         DATE          NOT NULL,
    -- Considera a�adir una clave primaria para la tabla de hechos,
    -- por ejemplo, un IDENTITY o una combinaci�n de claves.
    -- CONSTRAINT PK_FACTURAS PRIMARY KEY (numero_factura, fecha_factura, id_cliente_dw, id_producto_dw)
);
GO

-- Inserta los datos en la tabla FACT.FACTURAS
INSERT INTO FACT.FACTURAS (
    numero_factura,
    fecha_factura,
    id_cliente_dw,
    id_producto_dw,
    cantidad,
    total,
    descuento,
    Fecha_carga
)
SELECT DISTINCT
    -- Datos de la factura
    i.invoice_number        AS numero_factura,
    CAST(i.created_date AS DATE) AS fecha_factura,
    -- Relaci�n con cliente
    c.customer_id           AS id_cliente_dw,
    -- Relaci�n con producto
    p.product_id            AS id_producto_dw,
    -- M�tricas
    i.item_quantity         AS cantidad,
    i.item_total            AS total,
    i.item_discount         AS descuento,
    CAST(GETDATE() AS DATE) AS Fecha_carga
FROM [STG].[invoices] i
INNER JOIN [DIM].[DIM_CLIENTE] c
    ON i.customer_id = c.customer_id_native
INNER JOIN [DIM].[DIM_PRODUCTOS] p
    ON i.item_id = p.product_id_native;
GO

-- Opcional: Verifica los datos insertados
-- SELECT TOP 100 * FROM FACT.FACTURAS;





    



