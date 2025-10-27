-- models/staging/stg_weather_data.sql

with source as (
    -- Reference the raw table using the source() function
    select
        data, -- The JSON column
        loaded_at
    from {{ source('weather_raw_source', 'raw_weather') }} 
),

extracted_data as (
    -- Extract relevant fields from the JSON payload
    -- Adjust paths (->, ->>) based on the actual OpenWeatherMap JSON structure
    select
        (data ->> 'id')::integer as city_id,
        data ->> 'name' as city_name,
        (data -> 'coord' ->> 'lon')::numeric as longitude,
        (data -> 'coord' ->> 'lat')::numeric as latitude,

        -- Weather details (taking the first item in the 'weather' array)
        data -> 'weather' -> 0 ->> 'main' as weather_main,
        data -> 'weather' -> 0 ->> 'description' as weather_description,
        data -> 'weather' -> 0 ->> 'icon' as weather_icon,

        -- Main measurements
        (data -> 'main' ->> 'temp')::numeric as temperature_celsius,
        (data -> 'main' ->> 'feels_like')::numeric as feels_like_celsius,
        (data -> 'main' ->> 'temp_min')::numeric as temp_min_celsius,
        (data -> 'main' ->> 'temp_max')::numeric as temp_max_celsius,
        (data -> 'main' ->> 'pressure')::integer as pressure_hpa,
        (data -> 'main' ->> 'humidity')::integer as humidity_percent,

        -- Wind
        (data -> 'wind' ->> 'speed')::numeric as wind_speed_mps,
        (data -> 'wind' ->> 'deg')::integer as wind_direction_deg,

        -- Clouds
        (data -> 'clouds' ->> 'all')::integer as cloudiness_percent,

        -- Timestamp of the data observation
        -- Convert Unix timestamp (seconds) to timestamp with time zone
        to_timestamp((data ->> 'dt')::bigint) as observation_ts,

        -- System info
        (data -> 'sys' ->> 'country') as country_code,
        to_timestamp((data -> 'sys' ->> 'sunrise')::bigint) as sunrise_ts,
        to_timestamp((data -> 'sys' ->> 'sunset')::bigint) as sunset_ts,

        loaded_at -- Keep track of when the raw data was loaded

    from source
)

select * from extracted_data