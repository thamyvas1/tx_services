{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='append_new_columns',
    unique_key=['contract_id', 'start_date', 'end_date']
) }}

WITH base AS (
    SELECT 
        contract_id,
        created_at,
        tenant_id,
        tamedia_id,
        valid_from,
        valid_to,
        facturaperiode,
        is_daily_pass,
        subscription_class,
        subscription_status,
        next_billing_date,
        is_auto_renew,
        product_id,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ source('ext', 'subscriptions') }}  -- Ensure this references the correct table

    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * 
FROM base
