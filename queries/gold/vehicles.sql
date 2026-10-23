SELECT *
FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY retrieval_date DESC) = 1
