/*

This SQL script loads the t_Dim_Product dimension table using
Slow Changing Dimension detection technique in pure SQL for
efficient data loading.
*/

-- This handles type 1 SCD attributes
GO

select 'updating SCD type 1'

MERGE INTO dwh.t_Dim_Product AS target
USING staging.dim_product_delta AS source
ON (target.source_system_key = source.source_system_key)

WHEN MATCHED -- Update all existing rows for Type 1 changes

AND target.dwh_brand_id <> source.dwh_brand_id
    OR target.product_is_coupon <> source.product_is_coupon
    OR target.product_name <> source.product_name
    OR target.product_size <> source.product_size
    OR target.product_color <> source.product_color
    OR target.product_category <> source.product_category
    OR target.product_subcategory <> source.product_subcategory
    OR target.product_brand <> source.product_brand

THEN
UPDATE SET target.dwh_brand_id = source.dwh_brand_id,
    target.product_is_coupon = source.product_is_coupon,
    target.product_name = source.product_name,
    target.product_size = source.product_size,
    target.product_color = source.product_color,
    target.product_category = source.product_category,
    target.product_subcategory = source.product_subcategory,
    target.product_brand = source.product_brand,
  target.etl_modification_time = getdate() ;

GO
-- This handles existing records with type 2 attributes and new records
select 'updating SCD type 2 and new records'
INSERT INTO dwh.t_Dim_Product
(
    dwh_brand_id, product_is_coupon, product_name, product_size, product_color,
    product_category, product_subcategory, product_catalog_id, product_sku,
    product_is_from_stock, product_online_id,
    etl_creation_time, etl_modification_time,source_system_name,
    product_brand, purchase_price_no_vat,
    scd_start, scd_end, scd_active,source_system_key
)
    SELECT

        dwh_brand_id,
        product_is_coupon,
        product_name,
        product_size,
        product_color,
        product_category,
        product_subcategory,
        product_catalog_id,
        product_sku,
        product_is_from_stock,
        product_online_id,
        etl_creation_time,
        etl_modification_time,
        source_system_name,
        product_brand,
        purchase_price_no_vat,
        scd_start,
        scd_end,
        scd_active,
        source_system_key

    FROM
        (

            MERGE dwh.t_Dim_Product CM
            USING staging.dim_product_delta CS
            ON (CM.source_system_key = CS.source_system_key)
            WHEN NOT MATCHED THEN
            INSERT VALUES(
                    CS.dwh_brand_id,
                    CS.product_is_coupon,
                    CS.product_name,
                    CS.product_size,
                    CS.product_color,
                    CS.product_category,
                    CS.product_subcategory,
                    CS.product_catalog_id,
                    CS.product_sku,
                    CS.product_is_from_stock,
                    CS.product_online_id,
                    CS.etl_creation_time,
                    CS.etl_modification_time,
                    CS.source_system_name,
                    CS.product_brand,
                    CS.purchase_price_no_vat,
                    getdate(),
                    '3000-12-31 23:59:59',
                    '1',
                    CS.source_system_key)

            WHEN MATCHED AND CM.scd_active = '1'
                                             AND (ABS(isnull(CM.purchase_price_no_vat,0) - isnull(CS.purchase_price_no_vat,0)) > 0.01 ) THEN
            UPDATE SET CM.scd_active = '0', CM.scd_end = dateadd(s,-1,getdate()), CM.etl_modification_time = getdate()
            OUTPUT
      $ACTION Action_Out,
            CS.dwh_brand_id,
            CS.product_is_coupon,
            CS.product_name,
            CS.product_size,
            CS.product_color,
            CS.product_category,
            CS.product_subcategory,
            CS.product_catalog_id,
            CS.product_sku,
            CS.product_is_from_stock,
            CS.product_online_id,
            CS.etl_creation_time,
            CS.etl_modification_time,
            CS.source_system_name,
            CS.product_brand,
            CS.purchase_price_no_vat,
            getdate()-1  scd_start,
            '3000-12-31 23:59:59' scd_end,
            '1' scd_active,
            CS.source_system_key
        ) AS MERGE_OUT

    WHERE MERGE_OUT.Action_Out = 'UPDATE';
