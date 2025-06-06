{{
    config(
        materialized = "table",
        engine = "ReplicatedMergeTree",
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }} ON CLUSTER default",
    )
}}

SELECT
    po.id AS order_id,
    -- utc_to_mexico_tz('po.added') AS order_added,
    -- utc_to_mexico_tz('po.shipped_at') AS shipment_date,
    po.added AS order_added,
    po.shipped_at AS shipment_date,
    po.status AS order_status,
    po.amount / 100 AS total_price,
    po.payment_type AS payment_type,
    po.loan_id AS loan_id,
    po.shop_id AS shop_id,
    pmi.quantity AS items_quantity,
    pmi.promotion_id AS promotion_id
-- FROM kiwi_db_creds('kiwi', 'promotions_productorder') AS po
FROM postgresql('backend-postgresql-replica.db:5432', 'kiwi', 'promotions_productorder',
        "{{ env_var('PROD_DB_USER') }}", "{{ env_var('PROD_DB_PASS') }}",
        'public') AS po
--LEFT JOIN
--    kiwi_db_creds('kiwi', 'promotions_orderitem') AS pmi
--    ON po.id = pmi.order_id
LEFT JOIN postgresql('backend-postgresql-replica.db:5432', 'kiwi', 'promotions_orderitem',
        "{{ env_var('PROD_DB_USER') }}", "{{ env_var('PROD_DB_PASS') }}",
        'public') AS pmi ON po.id = pmi.order_id
