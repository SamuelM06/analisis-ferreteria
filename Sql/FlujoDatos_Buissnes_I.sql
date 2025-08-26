ALTER PROCEDURE Sp_Carga_Fact_facturas (@tipo_carga int =null)
as
begin

IF @tipo_carga = 0
BEGIN 
TRUNCATE TABLE [FACT].[FACTURAS]
END 

MERGE INTO FACT.FACTURAS AS Target
USING (
    -- Esta es tu fuente de datos, similar a tu SELECT original
    SELECT DISTINCT
        i.invoice_number        AS numero_factura,
        CAST(i.created_date AS DATE) AS fecha_factura,
        c.customer_id           AS id_cliente_dw,
        p.product_id            AS id_producto_dw,
        i.item_quantity         AS cantidad,
        i.item_total            AS total,
        i.item_discount         AS descuento,
        CAST(GETDATE() AS DATE) AS Fecha_carga
    FROM [STG].[invoices] i
    INNER JOIN [DIM].[DIM_CLIENTE] c
        ON i.customer_id = c.customer_id_native
    INNER JOIN [DIM].[DIM_PRODUCTOS] p
        ON i.item_id = p.product_id_native



) AS Source ON Target.numero_factura = Source.numero_factura
            AND Target.id_cliente_dw = Source.id_cliente_dw
            AND Target.id_producto_dw = Source.id_producto_dw -- Asegúrate de que esta combinación sea única para una factura

-- Cuando hay una coincidencia, actualiza los campos relevantes
WHEN MATCHED THEN
    UPDATE SET
        Target.fecha_factura = Source.fecha_factura,
        Target.cantidad      = Source.cantidad,
        Target.total         = Source.total,
        Target.descuento     = Source.descuento,
        Target.Fecha_carga   = Source.Fecha_carga -- Actualiza la fecha de carga a la actual

-- Cuando no hay una coincidencia, inserta la nueva fila
WHEN NOT MATCHED THEN
    INSERT (
        numero_factura,
        fecha_factura,
        id_cliente_dw,
        id_producto_dw,
        cantidad,
        total,
        descuento,
        Fecha_carga
    )
    VALUES (
        Source.numero_factura,
        Source.fecha_factura,
        Source.id_cliente_dw,
        Source.id_producto_dw,
        Source.cantidad,
        Source.total,
        Source.descuento,
        Source.Fecha_carga
    );

end 