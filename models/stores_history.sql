{% set source_table_alias = 'stores' %}
{% set unique_key = 'store_id' %}

{{
    config(
        alias=source_table_alias,
        materialized='incremental',
        unique_key=unique_key,
        dist=unique_key,
        sort='dbt_created_at',
        incremental_predicates = [
            "dbt_valid_to is null"
        ]
    )
}}

{% set fields_to_track = [
    'store_id', 'store_name', 'location', 'manager_name', 'contact_number'
] %}

-- Step 1: Define the source and existing records
WITH source_records AS (
    SELECT * FROM {{ ref(source_table_alias)}}
),
active_target_records AS (
    SELECT * FROM {{ this }} WHERE dbt_valid_to IS NULL
),
target_left_join_source AS (
    SELECT atr.*, src.{{ unique_key }} AS src_{{ unique_key }} 
    FROM active_target_records AS atr
    LEFT JOIN source_records AS src
        ON atr.{{ unique_key }} = src.{{ unique_key }}
),

-- Step 2: Identify Records That Are No Longer Present
to_deactivate_absent_records AS (
    SELECT
        tgt.{{ unique_key }},
        {{ prefix_fields('tgt', fields_to_track) }},
        -- dbt meta fields
        tgt.dbt_batch_id,
        tgt.dbt_created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        tgt.dbt_valid_from,
        CURRENT_TIMESTAMP AS dbt_valid_to
    FROM target_left_join_source tgt
    WHERE src_{{ unique_key }} IS NULL
),

-- Step 3: Identify Changing Records to Deactivate
to_deactivate_existing_records AS (
    SELECT
        atr.{{ unique_key }},
        {{ prefix_fields('atr', fields_to_track) }},
        -- dbt meta fields
        atr.dbt_batch_id,
        atr.dbt_created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        atr.dbt_valid_from,
        CURRENT_TIMESTAMP AS dbt_valid_to
    FROM active_target_records atr
    JOIN source_records src ON atr.{{ unique_key }} = src.{{ unique_key }}
    WHERE
        -- change data capture
        {{ compare_fields(fields_to_track, 'atr', 'src') }}

),

-- Step 4: Identify New Versions of Existing Records
new_versions_of_existing_records AS (
    SELECT
        src.{{ unique_key }},
        {{ prefix_fields('src', fields_to_track) }},
        -- dbt meta fields
        '{{ invocation_id }}' AS dbt_batch_id,
        CURRENT_TIMESTAMP AS dbt_created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        CURRENT_TIMESTAMP AS dbt_valid_from,
        NULL AS dbt_valid_to
    FROM source_records src
    JOIN active_target_records atr ON src.{{ unique_key }} = atr.{{ unique_key }}
    WHERE 
        -- change data capture
         {{ compare_fields(fields_to_track, 'atr', 'src') }}
),

-- Step 5: Identify New Records to Insert
new_records AS (
    SELECT
        src.{{ unique_key }},
        {{ prefix_fields('src', fields_to_track) }},
        -- dbt meta fields
        '{{ invocation_id }}' AS dbt_batch_id,
        CURRENT_TIMESTAMP AS dbt_created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        CURRENT_TIMESTAMP AS dbt_valid_from,
        NULL AS dbt_valid_to
    FROM source_records src
    LEFT JOIN active_target_records atr ON src.{{ unique_key }} = atr.{{ unique_key }}
    WHERE atr.{{ unique_key }} IS NULL
),

-- Final Step: Combine all transformations for incremental update
final AS (
    SELECT * FROM to_deactivate_existing_records
    UNION ALL
    SELECT * FROM to_deactivate_absent_records
    UNION ALL
    SELECT * FROM new_versions_of_existing_records
    UNION ALL
    SELECT * FROM new_records
)

SELECT * FROM final