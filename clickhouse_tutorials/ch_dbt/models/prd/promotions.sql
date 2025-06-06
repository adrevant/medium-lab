{{
    config(
        materialized = "table",
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }} ON CLUSTER default",
    )
}}

SELECT
    id AS promotion_id,
    uuid,
    -- utc_to_mexico_tz('added') AS promo_added,
    -- utc_to_mexico_tz('starts_at') AS start_date,
    -- utc_to_mexico_tz('ends_at') AS end_date,
    shipping_delay_days AS shipment_days,
    amount_shipping / 100 AS shipping_amount,
    amount_net / 100 AS item_price,
    description_short AS promo_name,
    round(discount_percentage_display * 100 / (amount_net / 100), 2)
        AS discount_percentage,
    discount_percentage_display AS discount_amount
-- FROM kiwi_db_creds('kiwi', 'promotions_promotion')
FROM postgresql('backend-postgresql-replica.db:5432', 'kiwi', 'promotions_promotion',
        "{{ env_var('PROD_DB_USER') }}", "{{ env_var('PROD_DB_PASS') }}",
        'public')
