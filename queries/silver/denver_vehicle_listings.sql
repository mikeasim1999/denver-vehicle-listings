WITH drf AS ( -- dealer rating fill
  SELECT
    `dealer.sourceId`,
    AVG(`dealer.dealerAverageRating`) AS average_dealer_rating,
    AVG(`dealer.dealerReviewRating`) AS average_review_rating,
    AVG(`dealer.dealerReviewCount`) AS average_review_count
  FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings
  GROUP BY `dealer.sourceId`
  ),
  bt AS ( -- base table
  SELECT
      -- ID's
      sdvl.unique_id AS transaction_listing_id, -- unique id (pk)
      sdvl.id AS listing_id,
      sdvl.`dealer.sourceId` AS dealer_id,
      REPLACE(CONCAT_WS('|'
          , COALESCE(CAST(sdvl.year AS STRING), '')
          , COALESCE(sdvl.make, '')
          , COALESCE(sdvl.model, '')
          , COALESCE(sdvl.trim, '')
          , COALESCE(sdvl.subTrim, '')
          , COALESCE(sdvl.engine, '')
          , COALESCE(CAST(sdvl.displacement AS STRING), '')
          , COALESCE(sdvl.driveType, '')
          , COALESCE(sdvl.transmission, '')
          , COALESCE(sdvl.fuel, '')
          , COALESCE(sdvl.bodyType, '')
          , COALESCE(sdvl.cabType, '')
          , COALESCE(sdvl.bedLength, '')
          ), ' ', ''
        ) AS vehicle_id,

      -- Dates
      CAST(sdvl.retrieval_date AS DATE) AS retrieval_date,
      CAST(sdvl.firstSeen AS DATE) AS first_seen,
      TO_DATE(ELEMENT_AT(FILTER(priceHistory, x -> x.listPrice IS NOT NULL), 1).date, 'MM/dd/yyyy') AS current_list_price_date,
      TO_DATE(ELEMENT_AT(FILTER(sdvl.priceHistory, x -> x.listPrice IS NOT NULL), -1).date, 'MM/dd/yyyy') AS original_list_price_date,
      CAST(sdvl.`dealer.dealerReviewDate` AS date) AS dealer_review_date,
      TO_DATE(ELEMENT_AT(FILTER(sdvl.`ownerHistory.history`, x -> x IS NOT NULL), 1).purchaseDate, 'MM/dd/yy') AS first_purchase_date,
      TO_DATE(ELEMENT_AT(FILTER(sdvl.`ownerHistory.history`, x -> x IS NOT NULL), -1).purchaseDate, 'MM/dd/yy') AS previous_purchase_date,
      TO_DATE(ELEMENT_AT(sdvl.`serviceHistory.history`, 1).date, 'MM/dd/yy') AS service_history_date,

      -- Vehicle Identification
      sdvl.vin,
      sdvl.year,
      sdvl.make,
      sdvl.model,
      sdvl.trim,
      sdvl.subTrim AS sub_trim,

      -- Vehicle Details
      ARRAY_JOIN(sdvl.topOptions, ', ') AS top_options,
      COALESCE(SIZE(sdvl.topOptions), 0) AS total_top_options,
      sdvl.mileage,
      sdvl.exteriorColor AS exterior_color,
      sdvl.interiorColor AS interior_color,
      sdvl.engine,
      sdvl.displacement,
      sdvl.drivetype AS drive_type,
      sdvl.transmission,
      sdvl.fuel,
      sdvl.mpgCity AS mpg_city,
      sdvl.mpgHighway AS mpg_highway,
      sdvl.mpgCombined AS mpg_combined,
      sdvl.bodytype AS body_type,
      sdvl.vehicleCondition AS vehicle_condition,
      COALESCE(sdvl.certified, false) AS certified,
      sdvl.cabType AS cab_type,
      sdvl.bedLength AS bed_length,

      -- Pricing
      sdvl.listPrice AS list_price,
      ELEMENT_AT(FILTER(priceHistory, x -> x.listPrice IS NOT NULL), -1).listPrice AS original_list_price,
      COALESCE(
        ELEMENT_AT(FILTER(sdvl.priceHistory, x -> x.listPrice IS NOT NULL), -1).listPrice -
        ELEMENT_AT(FILTER(sdvl.priceHistory, x -> x.listPrice IS NOT NULL), 1).listPrice,
        0) AS total_price_reduction,
      COALESCE(SIZE(FILTER(sdvl.priceHistory, x -> x.listPrice IS NOT NULL)), 0) AS total_price_history_entries,
      sdvl.currentPrice AS current_price,
      sdvl.onePrice AS one_price,
      sdvl.onePrice - sdvl.listPrice AS value_variance,
      CASE
        WHEN sdvl.onePrice - sdvl.listPrice >= 500 THEN 'Great'
        WHEN sdvl.onePrice - sdvl.listPrice < 500 AND sdvl.onePrice - sdvl.listPrice >= 0 THEN 'Good'
        WHEN sdvl.onePrice - sdvl.listPrice < 0 AND sdvl.onePrice - sdvl.listPrice >= -499 THEN 'Fair'
        WHEN sdvl.onePrice - sdvl.listPrice <= -500 THEN 'Bad'
      END AS value_variance_groups,
      INITCAP(sdvl.showHbvChange) AS show_hbv_change,

      -- Marketing & Metadata
      INITCAP(sdvl.badge) AS value_badge,
      sdvl.followCount AS follow_count,
      sdvl.stockNumber AS stock_number,
      sdvl.imageCount AS image_count,
      INITCAP(sdvl.recordType) AS record_type,
      INITCAP(sdvl.dealerType) AS dealer_type,
      sdvl.advantage AS advantage,
      sdvl.vdpUrl AS vdp_url,

      -- Scoring
      COALESCE(sdvl.sortScore, 0) AS sort_score,
      COALESCE(sdvl.baseScore, 0) AS base_score,
      COALESCE(sdvl.tpCostPerVdp, 0) AS tp_cost_per_vdp,
      COALESCE(sdvl.tpRetentionScore, 0) AS tp_retention_score,
      COALESCE(sdvl.cpc, 'Unspecified') AS cpc,

      -- Vehicle History Flags
      COALESCE(sdvl.oneOwner, false) AS one_owner,
      COALESCE(sdvl.noAccidents, false) AS no_accidents,
      COALESCE(sdvl.serviceRecords, false) AS service_records,
      COALESCE(sdvl.personalUse, false) AS personal_use,

      -- Atom Fields
      ARRAY_JOIN(sdvl.atomOtherOptions, ', ') AS atom_other_options,
      COALESCE(SIZE(sdvl.atomOtherOptions), 0) AS total_atom_other_options,
      ARRAY_JOIN(sdvl.atomTopOptions, ', ') AS atom_top_options,
      COALESCE(SIZE(sdvl.atomTopOptions), 0) AS total_atom_top_options,
      sdvl.chicletBadge AS chiclet_badge,

      -- Listing Status
      INITCAP(COALESCE(sdvl.listingStatus, 'Unspecified')) AS listing_status,
      sdvl.windowSticker AS window_sticker,
      sdvl.videoUrl AS video_url,
      DATEDIFF(
          CAST(sdvl.retrieval_date AS DATE),
          CAST(sdvl.firstSeen AS DATE)
      ) AS days_on_market,

      -- Dealer Information
      sdvl.`dealer.dealerInventoryUrl` AS dealer_inventory_url,
      sdvl.`dealer.micrositeUrl` AS dealer_listing_url,
      sdvl.`dealer.name` AS dealer_name,
      sdvl.`dealer.address` AS dealer_address,
      sdvl.`dealer.city` AS dealer_city,
      sdvl.`dealer.state` AS dealer_state,
      CAST(sdvl.`dealer.zip` AS NUMERIC) AS dealer_zip,
      sdvl.`dealer.phone` AS dealer_phone_raw,
      CONCAT(
        '(',
        LEFT(sdvl.`dealer.phone`, 3),
        ') ',
        SUBSTRING(sdvl.`dealer.phone`, 4, 3),
        '-',
        RIGHT(sdvl.`dealer.phone`, 4)
      ) AS dealer_phone_display,
      CAST(sdvl.`dealer.latitude` AS DECIMAL(10,8)) AS dealer_latitude,
      CAST(sdvl.`dealer.longitude` AS DECIMAL(11,8)) AS dealer_longitude,
      sdvl.`dealer.dealerLeadType` AS dealer_lead_type,
      sdvl.`dealer.showPriceHistory` AS dealer_show_price_history,
      COALESCE(COALESCE(sdvl.`dealer.dealerAverageRating`, drf.average_dealer_rating), 0) AS dealer_average_rating,
      sdvl.`dealer.dealerReviewComments` AS dealer_review_comments,
      INITCAP(sdvl.`dealer.dealerReviewReviewer`) AS dealer_review_reviewer,
      COALESCE(sdvl.`dealer.dealerReviewRating`, drf.average_review_rating) AS dealer_review_rating,
      COALESCE(sdvl.`dealer.dealerReviewCount`, drf.average_review_count) AS dealer_review_count,
      sdvl.`dealer.ddcValue` AS dealer_ddc_value,
      CASE
        WHEN INITCAP(sdvl.`dealer.dealerBadgingExperience`) = 'Toprated' THEN 'Top Rated'
        ELSE INITCAP(sdvl.`dealer.dealerBadgingExperience`)
      END AS dealer_badging_experience,
      COALESCE(sdvl.`dealer.showroom`, false) AS dealer_showroom,
      sdvl.distanceToDealer AS distance_to_dealer,
      CASE
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%acura%' THEN 'Acura'
        WHEN (LOWER(sdvl.`dealer.name`) LIKE '%chevrolet%' OR LOWER(sdvl.`dealer.name`) LIKE '%chevy%')
              AND (LOWER(sdvl.`dealer.name`) LIKE '%buick%' OR LOWER(sdvl.`dealer.name`) LIKE '%gmc%' OR sdvl.`dealer.name` LIKE '%GM%' OR LOWER(sdvl.`dealer.name`) LIKE '%cadillac%')
              OR (LOWER(sdvl.`dealer.name`) LIKE '%gmc%' AND LOWER(sdvl.`dealer.name`) LIKE '%buick%')
              OR (LOWER(sdvl.`dealer.name`) LIKE '%gmc%' AND LOWER(sdvl.`dealer.name`) LIKE '%cadillac%')
              THEN 'GM Multi-Brand'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%bentley%' THEN 'Bentley'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%bmw%' THEN 'BMW'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%cadillac%' THEN 'Cadillac'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%chevrolet%' OR LOWER(sdvl.`dealer.name`) LIKE '%chevy%' THEN 'Chevrolet'
        WHEN (LOWER(sdvl.`dealer.name`) LIKE '%dodge%' AND LOWER(sdvl.`dealer.name`) LIKE '%chrysler%')
          OR (LOWER(sdvl.`dealer.name`) LIKE '%jeep%' AND LOWER(sdvl.`dealer.name`) LIKE '%chrysler%')
          OR (LOWER(sdvl.`dealer.name`) LIKE '%dodge%' AND LOWER(sdvl.`dealer.name`) LIKE '%jeep%')
          OR (LOWER(sdvl.`dealer.name`) LIKE '%dodge%' AND LOWER(sdvl.`dealer.name`) LIKE '%ram%')
          THEN 'Stellantis Multi-Brand'
        WHEN (LOWER(sdvl.`dealer.name`) LIKE '%ford%' AND LOWER(sdvl.`dealer.name`) LIKE '%lincoln%') THEN 'Ford Multi-Brand'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%ford%' THEN 'Ford'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%genesis%' THEN 'Genesis'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%gmc%' THEN 'GMC'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%honda%' THEN 'Honda'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%hyundai%' THEN 'Hyundai'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%ineos grenadier%' THEN 'INEOS Grenadier'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%jaguar%' THEN 'Jaguar'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%jeep%' THEN 'Jeep'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%land rover%' THEN 'Land Rover'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%lincoln%' THEN 'Lincoln'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%infiniti%' THEN 'Infiniti'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%toyota%' THEN 'Toyota'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%mazda%' THEN 'Mazda'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%mini%' THEN 'Mini'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%nissan%' THEN 'Nissan'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%subaru%' THEN 'Subaru'
        WHEN (LOWER(sdvl.`dealer.name`) LIKE '%volkswagen%' OR LOWER(sdvl.`dealer.name`) LIKE '%vw%') THEN 'Volkswagen'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%kia%' THEN 'Kia'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%mercedes%' THEN 'Mercedes-Benz'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%lexus%' THEN 'Lexus'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%audi%' THEN 'Audi'
        WHEN LOWER(sdvl.`dealer.name`) LIKE '%volvo%' THEN 'Volvo'
        ELSE 'Independent'
      END AS dealer_brand,

      -- CPO Data
      COALESCE(sdvl.`cpoData.cpo_body`, 'Unspecified') AS cpo_body,
      COALESCE(sdvl.`cpoData.cpo_bullet_1`, 'Unspecified') AS cpo_bullet_1,
      COALESCE(sdvl.`cpoData.cpo_bullet_2`, 'Unspecified') AS cpo_bullet_2,
      COALESCE(sdvl.`cpoData.cpo_bullet_3`, 'Unspecified') AS cpo_bullet_3,
      COALESCE(sdvl.`cpoData.cpo_header`, 'Unspecified') AS cpo_header,
      sdvl.`cpoData.cpodata_id` AS cpo_data_id,
      sdvl.`cpoData.logo` AS cpo_logo,
      COALESCE(sdvl.`cpoData.make_name`, 'Unspecified') AS cpo_make_name,

      -- Monthly Payment Estimate
      sdvl.`monthlyPaymentEstimate.price` AS monthly_payment_price,
      (sdvl.`monthlyPaymentEstimate.downPaymentPercent` / 100) AS monthly_payment_down_payment_percent,
      (sdvl.`monthlyPaymentEstimate.interestRate` / 100) AS monthly_payment_interest_rate,
      sdvl.`monthlyPaymentEstimate.termInMonths` AS monthly_payment_term_in_months,
      sdvl.`monthlyPaymentEstimate.loanAmount` AS monthly_payment_loan_amount,
      sdvl.`monthlyPaymentEstimate.downPaymentAmount` AS monthly_payment_down_payment_amount,
      sdvl.`monthlyPaymentEstimate.monthlyPayment` AS monthly_payment_amount,

      -- Images
      sdvl.`images.baseUrl` AS images_base_url,
      sdvl.`images.firstPhoto.xLarge` AS images_first_photo_xlarge,
      SIZE(sdvl.`images.xlarge`) AS total_xlarge_images,
      sdvl.`images.firstPhoto.large` AS images_first_photo_large,
      SIZE(sdvl.`images.large`) AS total_large_images,
      sdvl.`images.firstPhoto.medium` AS images_first_photo_medium,
      SIZE(sdvl.`images.medium`) AS total_medium_images,
      sdvl.`images.firstPhoto.small` AS images_first_photo_small,
      SIZE(sdvl.`images.small`) AS total_small_images,

      -- Owner History
      CASE
        WHEN sdvl.`ownerHistory.text` = '2-Owner' THEN '2 Owners'
        WHEN sdvl.`ownerHistory.text` LIKE '%1-Owner%' THEN '1 Owner'
        WHEN sdvl.`ownerHistory.text` = 'Number of Owners' THEN 'Unknown'
        ELSE sdvl.`ownerHistory.text`
      END AS owner_history,
      ELEMENT_AT(FILTER(sdvl.`ownerHistory.history`, x -> x IS NOT NULL), 1).state AS first_owner_state,
      ELEMENT_AT(FILTER(sdvl.`ownerHistory.history`, x -> x IS NOT NULL), -1).state AS previous_owner_state,
      ARRAY_JOIN(ARRAY_DISTINCT(TRANSFORM(
          FILTER(sdvl.`ownerHistory.history`, x -> x.state IS NOT NULL),
          x -> x.state
      )), ', ') AS ownership_states,

      -- Accident History
      sdvl.`accidentHistory.text` AS accident_history,
      ARRAY_JOIN(sdvl.`accidentHistory.accidentSummary`, ', ') AS accident_history_detail,

      -- Service History
      sdvl.`serviceHistory.number` AS service_history_number,
      NULLIF(
        TRIM(
            ARRAY_JOIN(
                TRANSFORM(
                    SLICE(
                        SPLIT(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    ELEMENT_AT(sdvl.`serviceHistory.history`, 1).description,
                                    '<[^>]+>',
                                    ''
                                ),
                                '&#8226;',
                                '•'
                            ),
                            '•'
                        ),
                        2,
                        999
                    ),
                    x -> TRIM(x)
                ),
                ', '
            )
        ),
        ''
      ) AS service_history_description,
      CASE
        WHEN SIZE(`serviceHistory.history`) > 0
        THEN SIZE(
            FILTER(
                SLICE(
                    SPLIT(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                ELEMENT_AT(`serviceHistory.history`, 1).description,
                                '<[^>]+>',
                                ''
                            ),
                            '&#8226;',
                            '•'
                        ),
                        '•'
                      ),
                      2,
                      999
                    ),
                    x -> TRIM(x) != ''
                )
            )
        ELSE 0
      END AS total_recorded_services,
      ELEMENT_AT(sdvl.`serviceHistory.history`, 1).odometerReading AS service_history_odometer,
      INITCAP(SPLIT(ELEMENT_AT(`serviceHistory.history`, 1).source, ',')[0]) AS service_history_shop,
      ELEMENT_AT(sdvl.`serviceHistory.history`, 1).state AS service_history_state,

      -- Vehicle Use History
      sdvl.`vehicleUseHistory.text` AS vehicle_use_history,

      -- Reliability
      INITCAP(sdvl.`reliability.costBadge`) AS reliability_cost,
      INITCAP(sdvl.`reliability.riskBadge`) AS reliability_risk,
      sdvl.`reliability.averageCost` AS reliability_average_cost,
      INITCAP(sdvl.`reliability.overallReliabilityBadge`) AS reliability_overall_badge,
      sdvl.`reliability.reliabilityPlacement` AS reliability_placement,
      INITCAP(REPLACE(ARRAY_JOIN(sdvl.`reliability.reliabilityImpactFactors`, ', '), '_', ' ')) AS reliability_impact_factors
  FROM {CATALOG}.{SOURCE_SCHEMA}.denver_vehicle_listings AS sdvl
  LEFT JOIN drf
    ON sdvl.`dealer.sourceId` = drf.`dealer.sourceId`),
  mpgf AS ( -- miles per gallon fill
  SELECT
    vehicle_id,
    AVG(mpg_city) AS average_city_mpg,
    AVG(mpg_highway) AS average_highway_mpg,
    AVG(mpg_combined) AS average_combined_mpg
  FROM bt
  GROUP BY vehicle_id
  )
SELECT
  bt.transaction_listing_id,
  bt.listing_id,
  bt.dealer_id,
  bt.vehicle_id,
  bt.retrieval_date,
  bt.first_seen,
  bt.current_list_price_date,
  bt.original_list_price_date,
  bt.dealer_review_date,
  bt.first_purchase_date,
  bt.previous_purchase_date,
  bt.service_history_date,
  bt.vin,
  bt.year,
  bt.make,
  bt.model,
  bt.trim,
  bt.sub_trim,
  bt.top_options,
  bt.total_top_options,
  bt.mileage,
  bt.exterior_color,
  bt.interior_color,
  bt.engine,
  bt.displacement,
  bt.drive_type,
  bt.transmission,
  bt.fuel,
  COALESCE(bt.mpg_city, mpgf.average_city_mpg) AS mpg_city,
  COALESCE(bt.mpg_highway, mpgf.average_highway_mpg) AS mpg_highway,
  COALESCE(bt.mpg_combined, mpgf.average_combined_mpg) AS mpg_combined,
  bt.body_type,
  bt.vehicle_condition,
  bt.certified,
  bt.cab_type,
  bt.bed_length,
  bt.list_price,
  bt.total_price_reduction,
  bt.total_price_history_entries,
  bt.current_price,
  bt.original_list_price,
  bt.one_price,
  bt.value_variance,
  bt.value_variance_groups,
  bt.show_hbv_change,
  bt.value_badge,
  bt.follow_count,
  bt.stock_number,
  bt.image_count,
  bt.record_type,
  bt.dealer_type,
  bt.advantage,
  bt.vdp_url,
  bt.sort_score,
  bt.base_score,
  bt.tp_cost_per_vdp,
  bt.tp_retention_score,
  bt.cpc,
  bt.one_owner,
  bt.no_accidents,
  bt.service_records,
  bt.personal_use,
  bt.atom_other_options,
  bt.total_atom_other_options,
  bt.atom_top_options,
  bt.total_atom_top_options,
  bt.chiclet_badge,
  bt.listing_status,
  bt.window_sticker,
  bt.video_url,
  bt.days_on_market,
  bt.dealer_inventory_url,
  bt.dealer_listing_url,
  bt.dealer_name,
  bt.dealer_address,
  bt.dealer_city,
  bt.dealer_state,
  bt.dealer_zip,
  bt.dealer_phone_raw,
  bt.dealer_phone_display,
  bt.dealer_latitude,
  bt.dealer_longitude,
  bt.dealer_lead_type,
  bt.dealer_show_price_history,
  bt.dealer_average_rating,
  bt.dealer_review_comments,
  bt.dealer_review_reviewer,
  bt.dealer_review_rating,
  bt.dealer_review_count,
  bt.dealer_ddc_value,
  bt.dealer_badging_experience,
  bt.dealer_showroom,
  bt.distance_to_dealer,
  bt.dealer_brand,
  bt.cpo_body,
  bt.cpo_bullet_1,
  bt.cpo_bullet_2,
  bt.cpo_bullet_3,
  bt.cpo_header,
  bt.cpo_data_id,
  bt.cpo_logo,
  bt.cpo_make_name,
  bt.monthly_payment_price,
  bt.monthly_payment_down_payment_percent,
  bt.monthly_payment_interest_rate,
  bt.monthly_payment_term_in_months,
  bt.monthly_payment_loan_amount,
  bt.monthly_payment_down_payment_amount,
  bt.monthly_payment_amount,
  bt.images_base_url,
  bt.images_first_photo_xlarge,
  bt.total_xlarge_images,
  bt.images_first_photo_large,
  bt.total_large_images,
  bt.images_first_photo_medium,
  bt.total_medium_images,
  bt.images_first_photo_small,
  bt.total_small_images,
  bt.owner_history,
  bt.first_owner_state,
  bt.previous_owner_state,
  bt.ownership_states,
  bt.accident_history,
  bt.accident_history_detail,
  bt.service_history_number,
  bt.service_history_description,
  bt.total_recorded_services,
  bt.service_history_odometer,
  bt.service_history_shop,
  bt.service_history_state,
  bt.vehicle_use_history,
  bt.reliability_cost,
  bt.reliability_risk,
  bt.reliability_average_cost,
  bt.reliability_overall_badge,
  bt.reliability_placement,
  bt.reliability_impact_factors
FROM bt
LEFT JOIN mpgf
  ON bt.vehicle_id = mpgf.vehicle_id
-- << INCREMENTAL >> --
LEFT JOIN
  {CATALOG}.{TARGET_SCHEMA}.denver_vehicle_listings AS tdvl
  ON bt.transaction_listing_id = tdvl.transaction_listing_id
WHERE
  (
    tdvl.transaction_listing_id IS NULL
    OR bt.retrieval_date >= current_date() - INTERVAL '{incrementalInterval}' DAYS
  )
