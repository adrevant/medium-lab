{{
    config(
        materialized = "incremental",
        unique_key = "reservation_id",
        primary_key = '(reservation_q, transaction_payment_result, transaction_type, user_id, reservation_id)',
        order_by = '(reservation_q,  transaction_payment_result, transaction_type, user_id, reservation_id)',
        settings = {'allow_nullable_key': 1,
                    'replicated_can_become_leader': True,
                    'max_suspicious_broken_parts': 1},
        partition_by = 'reservation_year',
        incremental_strategy = 'delete+insert',
    )
}}

SELECT
    id AS reservation_id,
    -- utc_to_mexico_tz('last_mod') AS last_mod,
    last_mod,
    shop_id AS user_id,
    -- utc_to_mexico_tz('added') AS transaction_added,
    added AS reservation_added,
    -- date_trunc('year', utc_to_mexico_tz('added')) AS txn_year_added,
    -- date_trunc('quarter', utc_to_mexico_tz('added')) AS txn_q_added,
    date_trunc('year', added) AS reservation_year,
    date_trunc('quarter', added) AS reservation_q,
    card_id,
    is_cancelled,
    is_failed, -- noqa: L029
    is_reversed,
    --CASE
    --    WHEN
    --        processed_at IS NOT NULL
    --        THEN utc_to_mexico_tz('processed_at')
    --    ELSE processed_at
    --END AS processed_at,
    -- utc_to_mexico_tz('processed_at') AS processed_at,
    processed_at,
    reference,
    amount * 1. / 100 AS amount,
    CASE
        WHEN result = 0 THEN 'APPROVED'
        WHEN result = 1 THEN 'DECLINED'
        WHEN result = 2 THEN 'ERROR'
        WHEN result = 3 THEN 'TIMEOUT'
        WHEN result = 4 THEN 'PENDING'
        ELSE 'UNKNOWN_RESULT'
    END AS transaction_payment_result,
    CASE
        WHEN type = 0 THEN 'CASH'
        WHEN type = 1 THEN 'CHIP'
        WHEN type = 2 THEN 'SWIPE'
        WHEN type = 3 THEN 'MOBILE'
        WHEN type = 6 THEN 'CONTACTLESS'
        ELSE 'UNKNOWN_TYPE'
    END AS transaction_type,
    commissions
    /*dateDiff('minute',
             utc_to_mexico_tz('added'),
             utc_to_mexico_tz('last_mod'))
    AS last_mod_minutes_diff*/
-- FROM kiwi_db_creds('kiwi', 'transactions_transaction')
FROM postgresql('backend-postgresql-replica.db:5432', 'kiwi', 'transactions_transaction',
        "{{ env_var('PROD_DB_USER') }}", "{{ env_var('PROD_DB_PASS') }}",
        'public')
{% if is_incremental() %}

    -- WHERE last_mod > (SELECT MAX(last_mod) FROM {{ this }})
    WHERE last_mod > (
        SELECT MAX(last_mod) FROM {{ this }}
        WHERE
            -- txn_year_added >= date_trunc('year',
            -- now('America/Mexico_City') - INTERVAL '1 day')
            reservation_year
            >= date_trunc(
                'year', now('America/Mexico_City') - INTERVAL '365 day'
            )
            -- no record can have an smaller last_mod value than
            -- its txn_added val. '-1 day' for update every new year
            -- or '-365 day' for all txns added last year
    )

{% endif %}
