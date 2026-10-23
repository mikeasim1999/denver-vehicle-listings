-- dealer
WITH drn AS ( -- dealer_row_numbering
  SELECT
    dealer_id,
    dealer_name,
    dealer_brand,
    dealer_type,
    dealer_inventory_url,
    dealer_listing_url,
    dealer_state,
    dealer_city,
    dealer_zip,
    dealer_phone_display,
    dealer_latitude,
    dealer_longitude,
    dealer_average_rating,
    dealer_review_count,
    ROW_NUMBER() OVER (PARTITION BY dealer_id ORDER BY retrieval_date DESC) AS row_num
  FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings)
SELECT
  dealer_id,
  dealer_name,
  dealer_brand,
  dealer_type,
  dealer_inventory_url,
  dealer_listing_url,
  dealer_state,
  dealer_city,
  dealer_zip,
  dealer_phone_display,
  dealer_latitude,
  dealer_longitude,
  dealer_average_rating,
  dealer_review_count
FROM drn
WHERE row_num = 1
