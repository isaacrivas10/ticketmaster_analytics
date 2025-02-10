{{ config(
    materialized='table',
    partition_by={
        "field": "start_datetime",
        "data_type": "timestamp",
        "granularity": "hour"
    }
) }}
WITH event_base AS (
    SELECT
        id,
        event_name,
        start_datetime,
        end_datetime,
        sales_start_datetime,
        event_timezone,
        event_status_code,
        is_multi_day_event,
        
        -- Event duration in hours
        TIMESTAMP_DIFF(end_datetime, start_datetime, MINUTE) / 60 AS event_duration_hours,

        -- Booking window (Lead time in days)
        DATE_DIFF(start_datetime, sales_start_datetime, DAY) AS booking_window_days,

        -- Event time category
        CASE 
            WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 17 AND 21 THEN 'Evening'
            ELSE 'Late Night'
        END AS event_time_category,

        -- Weekend vs. Weekday event
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM start_datetime) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS event_day_type,

        -- Event seasonality (Winter, Spring, Summer, Fall)
        CASE 
            WHEN EXTRACT(MONTH FROM start_datetime) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM start_datetime) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM start_datetime) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM start_datetime) IN (9, 10, 11) THEN 'Fall'
        END AS event_season,

        -- Event category breakdown
        classification.segment AS event_segment,
        classification.genre AS event_genre,
        classification.sub_genre AS event_sub_genre,
        classification.type AS event_type,
        classification.subtype AS event_subtype,

        -- Price details
        price_details.currency AS currency,
        price_details.min AS min_price,
        price_details.max AS max_price,

        -- Price spread
        price_details.max - price_details.min AS price_spread,

        -- Discount category (To check price difference in regards of discounted tiers)
        CASE 
            WHEN (price_details.max - price_details.min) > (price_details.max * 0.5) THEN 'High Discount'
            WHEN (price_details.max - price_details.min) BETWEEN (price_details.max * 0.2) AND (price_details.max * 0.5) THEN 'Moderate Discount'
            ELSE 'Low Discount'
        END AS discount_category,
        
        -- Venue & location details
        venue.id AS venue_id,
        venue.name AS venue_name,
        venue.city AS venue_city,
        venue.state AS venue_state,
        venue.country AS venue_country,
        venue.country_code AS venue_country_code,
        venue.latitude AS latitude,
        venue.longitude AS longitude
    FROM {{ ref('stg_events') }}
)

SELECT
    *
FROM event_base
