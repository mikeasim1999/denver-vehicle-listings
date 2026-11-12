SELECT 
  sdvl.transaction_listing_id
  ,sdvl.listing_id
  ,sdvl.dealer_id
  ,sdvl.vehicle_id
  ,sdvl.retrieval_date
  ,sdvl.first_seen
  ,sdvl.vin
  ,sdvl.owner_history
  ,sdvl.accident_history
  ,sdvl.total_top_options
  ,sdvl.mileage
  ,sdvl.mpg_city
  ,sdvl.mpg_highway
  ,sdvl.mpg_combined
  ,sdvl.list_price
  ,sdvl.original_list_price
  ,sdvl.current_price
  ,sdvl.one_price
  ,sdvl.value_variance
  ,sdvl.value_variance_groups
  ,sdvl.total_price_reduction
  ,sdvl.total_price_history_entries
  ,sdvl.days_on_market
  ,sdvl.total_recorded_services
FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings AS sdvl
-- << INCREMENTAL >> --
LEFT JOIN
  {CATALOG}.{TARGET_SCHEMA}.daily_listings AS tdvl
  ON sdvl.transaction_listing_id = tdvl.transaction_listing_id
WHERE
  (
    tdvl.transaction_listing_id IS NULL
    OR sdvl.retrieval_date >= current_date() - INTERVAL '{incrementalInterval}' DAYS
  )
