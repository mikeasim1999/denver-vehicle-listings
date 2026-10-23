SELECT *
FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings
-- << INCREMENTAL >> --
LEFT JOIN
  {CATALOG}.{TARGET_SCHEMA}.daily_listings AS tdl
  ON {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings.transaction_listing_id = tdl.transaction_listing_id
WHERE
  (
    tdl.transaction_listing_id IS NULL
    OR {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings.retrieval_date >= current_date() - INTERVAL '{incrementalInterval}' DAYS
  )
