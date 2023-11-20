--- total number of trips ending in service_zones 'Airports' or 'EWR'
select 
    count(*) as total_trips
from {{ ref('mart__fact_all_taxi_trips') }} as t
    join {{ ref('mart__dim_locations') }} as l
        on t.dolocationid = l.locationid
where l.service_zone in ('Airports', 'EWR')

--duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_ending_at_airport.sql"> answers/taxi_trips_ending_at_airport.txt