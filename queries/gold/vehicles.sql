SELECT 
    vehicle_id
    ,year
    ,make
    ,model
    ,trim
    ,sub_trim
    ,engine
    ,displacement
    ,drive_type
    ,transmission
    ,fuel
    ,body_type
    ,cab_type
    ,bed_length
FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY retrieval_date DESC) = 1
