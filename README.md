# dbt Incremental Model for Historical Data Management

This repository provides an incremental model in dbt for managing historical data with support for change data capture (CDC). It includes macros for DRY (Don't Repeat Yourself) coding and an example model for handling changes and capturing historical data efficiently.

## Features
- **Incremental Model**: Handles data changes efficiently with incremental materialization.
- **Macros**: Modularized logic to keep the code DRY.
- **CDC Support**: Identifies new, changed, and removed records using change data capture techniques.

## Macros

#### `compare_fields`
Compares corresponding fields between two datasets and returns a logical expression for identifying changes.

**Parameters:**
- `fields` (list of strings): Fields to compare.
- `prefix1` (string): Prefix (or alias) for the first dataset.
- `prefix2` (string): Prefix (or alias) for the second dataset.

```sql
{% macro compare_fields(fields, prefix1, prefix2) %}
    {% set conditions = [] %}
    {% for field in fields %}
        {{ conditions.append(prefix1 ~ '.' ~ field ~ ' <> ' ~ prefix2 ~ '.' ~ field) }}
    {% endfor %}
    {{ return(' OR '.join(conditions)) }}
{% endmacro %}
```

#### `prefix_fields`
Generates a list of fields prefixed with a specified alias.

**Parameters:**
- `alias` (string): The alias to prefix the fields.
- `fields` (list of strings): List of field names to prefix.

```sql
{% macro prefix_fields(alias, fields) %}
    {% set prefixed_fields = [] %}
    {% for field in fields %}
        {{ prefixed_fields.append(alias ~ '.' ~ field) }}
    {% endfor %}
    {{ return(prefixed_fields | join(', ')) }}
{% endmacro %}
```

#### Example Model: `products_history`

This model manages a historical table for product data, handling changes incrementally. It:
1. Tracks active records.
2. Identifies records that are no longer present.
3. Deactivates outdated versions of records with changes.
4. Inserts new records or new versions of existing records.

**SQL Code**:

```sql
{% set source_table_alias = 'products' %}
{% set unique_key = 'product_id' %}

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
    'product_name', 'description', 'price', 'stock_quantity', 'category_id'
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
```

## Usage
1. **Macros**: Copy the macros to a `.sql` file inside your `macros` folder.
2. **Model**: Place the provided SQL model inside your `models` folder.
3. **Run dbt**: Execute `dbt run` to apply the incremental model.

## Customization
- Adjust the `fields_to_track` variable to include the fields that need change tracking.
- Modify the `unique_key` to match your data schema's primary key.

## Creating New Incremental Models for another data sources

To create a new incremental model for another data source using dbt, you only need to configure a few parameters specific to your new source. Here’s a straightforward guide to setting up your model:

1. **Define Source Table Alias**: Specify the alias for the source table from which the data will be pulled. This alias is used to reference the table in your SQL code.

    ```sql
    {% set source_table_alias = '<new_source_table>' %}
    ```

2. **Set Unique Key**: Identify the unique key of the source table. This key is used to detect changes and manage data over incremental runs effectively.

    ```sql
    {% set unique_key = '<unique_key_of_new_source_table>' %}
    ```

3. **List Fields to Track**: Determine which fields you want to track for changes. These fields are used in the change detection logic of the incremental model.

    ```sql
    {% set fields_to_track = [
        '<field1>', '<field2>', '<field3>', ...  // List all fields you want to track separated by commas
    ] %}
    ```

### Example New Model

Below is a new incremental model for `stores` table:

```sql
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

-- Include the SQL logic for the incremental model using the defined variables and macros
```

## License
This project is licensed under the MIT License.
