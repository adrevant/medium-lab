{{
    config(
        unique_key = 'user_id',
        pre_hook = 'TRUNCATE TABLE IF EXISTS {{ this }} ON CLUSTER default'
    )
}}

SELECT
    u.id AS user_id,
    u.added AS date_created,
    cc.country_name AS country_name
-- FROM kiwi_db_creds('kiwi', 'contracts_shop') AS u
FROM postgresql('backend-postgresql-replica.db:5432', 'kiwi', 'contracts_shop',
        "{{ env_var('PROD_DB_USER') }}", "{{ env_var('PROD_DB_PASS') }}",
        'public') AS u
LEFT JOIN {{ ref('country_codes') }} AS cc ON u.market_id = cc.country_code
