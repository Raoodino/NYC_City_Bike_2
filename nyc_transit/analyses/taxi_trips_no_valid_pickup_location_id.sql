
-- finds taxi trips which don’t have a pick up location_id in the locations table
SELECT taxi_trip.*
FROM {{ ref('mart__fact_all_taxi_trips')}} taxi_trip
LEFT JOIN {{ ref('mart__dim_locations')}} location
ON taxi_trip.PulocationID = location.locationID 
WHERE location.locationID is NULL

-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_no_valid_pickup_location_id.sql"> answers/taxi_trips_no_valid_pickup_location_id.txt