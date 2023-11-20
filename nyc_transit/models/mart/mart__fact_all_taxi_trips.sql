with trips_renamed as
(
    select 'fhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__fhv_tripdata') }}
    union all
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__fhvhv_tripdata') }}
    union all
    select 'green' as type, lpep_pickup_datetime, lpep_dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__green_tripdata') }}
    union all
    select 'yellow' as type, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__yellow_tripdata') }}
)

SELECT
    type,
    pickup_datetime,
    dropoff_datetime,
    -- set abnormal data to null
    CASE 
        WHEN datediff('minute', pickup_datetime, dropoff_datetime) BETWEEN 0 AND 9000
        THEN datediff('minute', pickup_datetime, dropoff_datetime) 
        ELSE NULL 
    END as duration_min,
    CASE 
        WHEN datediff('second', pickup_datetime, dropoff_datetime) BETWEEN 0 AND 540000
        THEN datediff('second', pickup_datetime, dropoff_datetime) 
        ELSE NULL 
    END as duration_sec,
    pulocationid,
    dolocationid,
FROM trips_renamed