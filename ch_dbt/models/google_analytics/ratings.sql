{{
    config(
        materialized = "table",
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }} ON CLUSTER default",
    )
}}

SELECT
    id AS score_uuid,
    'app' AS product,
    score,
    shop_id AS user_id,
    comment,
    parseDateTime64BestEffortOrNull(createdAt, 4, 'America/Mexico_City')
        AS score_added
-- FROM kiwi_db_creds('kiwipg', 'rating', 'kiwipg-rw.meta')
FROM postgresql('kiwipg-rw.meta:5432', 'kiwipg', 'rating',
        "{{ env_var('PROD_DB2_USER') }}", "{{ env_var('PROD_DB2_PASS') }}",
        'public')
