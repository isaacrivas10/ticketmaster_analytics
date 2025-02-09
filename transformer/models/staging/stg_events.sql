{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns',
    partition_by={
        "field": "start_datetime",
        "data_type": "timestamp",
        "granularity": "hour"
    }
) }} 

WITH deduplicated AS (
    SELECT
        id,
        name AS event_name,
        type AS event_type,
        locale,
        description,
        info,
        ticketing_id,

        -- Convert to DATE & TIMESTAMP
        SAFE_CAST(sales_public_startDateTime AS TIMESTAMP) AS sales_start_datetime,
        SAFE_CAST(sales_public_endDateTime AS TIMESTAMP) AS sales_end_datetime,
        SAFE_CAST(dates_start_localDate AS DATE) AS start_local_date,
        SAFE_CAST(dates_start_dateTime AS TIMESTAMP) AS start_datetime,
        SAFE_CAST(dates_end_localDate AS DATE) AS end_local_date,
        SAFE_CAST(dates_end_dateTime AS TIMESTAMP) AS end_datetime,

        dates_timezone AS event_timezone,
        dates_status_code AS event_status_code,
        SAFE_CAST(dates_spanMultipleDays AS BOOLEAN) AS is_multi_day_event,
        SAFE_CAST(ageRestrictions_legalAgeEnforced AS BOOLEAN) AS legal_age_enforced,
        ticketing_safeTix_enabled AS safeTix_enabled,
        ticketing_allInclusivePricing_enabled AS all_inclusive_pricing,

        -- Extract first presale start date as fallback if public sale date is missing
        COALESCE(
            sales_public_startDateTime,
            (
                SELECT
                    presale.element.startdatetime
                FROM
                    UNNEST(sales_presales.list) AS presale
                LIMIT
                    1
            )
        ) AS adjusted_sales_start_datetime,
        -- Extract classifications
        (
            SELECT
                AS STRUCT classification.element.primary AS is_primary,
                classification.element.segment.name AS segment,
                classification.element.genre.name AS genre,
                classification.element.subGenre.name AS sub_genre,
                classification.element.type.name AS type,
                classification.element.subtype.name AS subtype,
            FROM
                UNNEST(classifications.list) AS classification
            LIMIT
                1 -- Since the array usually contains a single record
        ) AS classification,
        -- Extract price details
        (
            SELECT
                AS STRUCT price.element.currency,
                price.element.min,
                price.element.max
            FROM
                UNNEST(priceRanges.list) AS price
            LIMIT
                1 -- Since the array usually contains a single record
        ) AS price_details,
        -- Extract first venue details
        (
            SELECT
                AS STRUCT venue.element.id AS id,
                venue.element.name AS name,
                venue.element.postalCode AS postal_code,
                venue.element.city.name AS city,
                venue.element.state.name AS state,
                venue.element.state.statecode AS state_code,
                venue.element.country.name AS country,
                venue.element.country.countrycode AS country_code,
                venue.element.location.latitude AS latitude,
                venue.element.location.longitude AS longitude,
                venue.element.locale AS locale,
                venue.element.timezone AS timezone,
                venue.element.type AS type
            FROM
                UNNEST(_embedded_venues.list) AS venue
            LIMIT
                1
        ) AS venue,
        -- Extract all attraction details
        ARRAY(
            SELECT
                AS STRUCT attraction.element.id AS id,
                attraction.element.name AS name,
                attraction.element.type AS type,
                attraction.element.locale AS locale,
                (
                    SELECT
                        AS STRUCT classification.element.segment.name AS segment,
                        classification.element.genre.name AS genre,
                        classification.element.subGenre.name AS sub_genre,
                        classification.element.type.name AS type,
                        classification.element.subtype.name AS subtype
                    FROM
                        UNNEST(attraction.element.classifications.list) AS classification
                    LIMIT
                        1
                ) AS classification
            FROM
                UNNEST(_embedded_attractions.list) AS attraction
        ) AS attractions,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                dates_start_dateTime DESC
        ) AS row_num
    FROM
        {{ source('raw', 'raw_events') }}
)

SELECT * EXCEPT(row_num) FROM deduplicated WHERE row_num = 1

{% if is_incremental() %}
    AND start_datetime > (
        SELECT MAX(start_datetime) FROM {{ this }}
    ) 
{% endif %}