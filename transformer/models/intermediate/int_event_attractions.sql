{{ config(
    materialized='incremental',
    unique_key=['event_id', 'attraction_id'],
    on_schema_change='sync_all_columns'
) }}

WITH exploded_attractions AS (
    SELECT 
        e.id AS event_id,
        a.id AS attraction_id,
        a.name AS attraction_name,
        a.locale AS attraction_locale,

        a.classification.segment AS segment,
        a.classification.genre AS genre,
        a.classification.sub_genre AS sub_genre,
        a.classification.type AS type,
        a.classification.subtype AS subtype
    FROM {{ ref('stg_events') }} e
    LEFT JOIN UNNEST(e.attractions) a
)

SELECT * FROM exploded_attractions
{% if is_incremental() %}
WHERE event_id NOT IN (SELECT DISTINCT event_id FROM {{ this }})
{% endif %}
