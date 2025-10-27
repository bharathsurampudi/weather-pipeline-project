-- models/marts/fct_weather_daily.sql

with staging as (
    -- Reference the staging model
    select * from {{ ref('stg_weather_data') }}
)

select 
    -- Select key fields for the final table
    city_id,
    city_name,
    country_code,
    observation_ts,
    date(observation_ts) as observation_date, -- Extract date part
    weather_main,
    weather_description,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    wind_speed_mps,
    cloudiness_percent,
    sunrise_ts,
    sunset_ts,
    loaded_at

from staging
-- Optional: Add filtering or aggregation logic here if needed for a daily summary
-- e.g., using DISTINCT ON or ROW_NUMBER() if multiple observations per day exist
order by 
    city_name, 
    observation_ts desc -- Keep the latest observation if duplicates exist per day? Define logic clearly.