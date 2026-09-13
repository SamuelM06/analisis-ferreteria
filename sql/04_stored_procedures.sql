-- ==============================================================================
-- 04. PROCEDIMIENTOS ALMACENADOS PARA CARGA INCREMENTAL
-- ==============================================================================
USE ferreteria_db;
GO

CREATE OR ALTER PROCEDURE Sp_Carga_Fact_facturas (
    @tipo_carga INT = NULL  -- 0 = Truncar y recargar completa, NULL/1 = Carga incremental con MERGE
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @tipo_carga = 0
    BEGIN 
        TRUNCATE TABLE [FACT].[FACTURAS];
    END 

    MERGE INTO FACT.FACTURAS AS Target
    USING (
        SELECT DISTINCT
            i.invoice_number        AS numero_factura,
            CAST(i.created_date AS DATE) AS fecha_factura,
            c.customer_id           AS id_cliente_dw,
            p.product_id            AS id_producto_dw,
            CAST(i.item_quantity AS INT) AS cantidad,
            i.item_total            AS total,
            i.item_discount         AS descuento,
            CAST(GETDATE() AS DATE) AS fecha_carga
        FROM [STG].[invoices] i
        INNER JOIN [DIM].[DIM_CLIENTE] c
            ON i.customer_id = c.customer_id_native
        INNER JOIN [DIM].[DIM_PRODUCTOS] p
            ON i.item_id = p.product_id_native
    ) AS Source 
        ON Target.numero_factura = Source.numero_factura
        AND Target.id_cliente_dw = Source.id_cliente_dw
        AND Target.id_producto_dw = Source.id_producto_dw

    WHEN MATCHED THEN
        UPDATE SET
            Target.fecha_factura = Source.fecha_factura,
            Target.cantidad      = Source.cantidad,
            Target.total         = Source.total,
            Target.descuento     = Source.descuento,
            Target.fecha_carga   = Source.fecha_carga

    WHEN NOT MATCHED THEN
        INSERT (
            numero_factura,
            fecha_factura,
            id_cliente_dw,
            id_producto_dw,
            cantidad,
            total,
            descuento,
            fecha_carga
        )
        VALUES (
            Source.numero_factura,
            Source.fecha_factura,
            Source.id_cliente_dw,
            Source.id_producto_dw,
            Source.cantidad,
            Source.total,
            Source.descuento,
            Source.fecha_carga
        );
END
GO
