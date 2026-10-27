-- =============================================================================
-- Data Quality Checks — Table DDL & Seed Rules
-- Creates the data_quality_checks table and inserts initial validation rules.
-- Run once per schema where validation is needed.
-- =============================================================================

CREATE TABLE IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}.data_quality_checks (
  rule_id       INT,
  target_table  STRING,
  rule_name     STRING,
  sql_expression STRING,
  severity      STRING
);

-- =============================================================================
-- Silver Layer Rules — denver_vehicle_listings
-- Each sql_expression identifies INVALID rows (failures).
-- A non-zero count from: SELECT COUNT(*) WHERE <sql_expression> means a failure.
-- =============================================================================

INSERT INTO {CATALOG}.{TARGET_SCHEMA}.data_quality_checks
VALUES
  (1, 'denver_vehicle_listings', 'null_transaction_listing_id', 'transaction_listing_id IS NULL', 'CRITICAL'),
  (2, 'denver_vehicle_listings', 'null_listing_id',             'listing_id IS NULL',             'CRITICAL'),
  (3, 'denver_vehicle_listings', 'null_retrieval_date',         'retrieval_date IS NULL',          'CRITICAL'),
  (4, 'denver_vehicle_listings', 'null_vehicle_id',             'vehicle_id IS NULL',              'CRITICAL'),
  (5, 'denver_vehicle_listings', 'null_dealer_id',              'dealer_id IS NULL',               'CRITICAL');


-- =============================================================================
-- Gold Layer Rules
-- Run against each gold table's DataFrame before write.
-- =============================================================================

-- ── vehicles ─────────────────────────────────────────────────────────────────
INSERT INTO {CATALOG}.{TARGET_SCHEMA}.data_quality_checks
VALUES
  (1, 'vehicles', 'null_vehicle_id',        'vehicle_id IS NULL',      'CRITICAL'),
  (2, 'vehicles', 'duplicate_vehicle_id',   'vehicle_id IN (SELECT vehicle_id FROM vehicles GROUP BY vehicle_id HAVING COUNT(*) > 1)', 'CRITICAL'),
  (3, 'vehicles', 'null_listing_id',        'listing_id IS NULL',      'WARNING'),
  (4, 'vehicles', 'negative_list_price',    'list_price < 0',          'WARNING');

-- ── dealers ──────────────────────────────────────────────────────────────────
INSERT INTO {CATALOG}.{TARGET_SCHEMA}.data_quality_checks
VALUES
  (5, 'dealers', 'null_dealer_id',          'dealer_id IS NULL',       'CRITICAL'),
  (6, 'dealers', 'duplicate_dealer_id',     'dealer_id IN (SELECT dealer_id FROM dealers GROUP BY dealer_id HAVING COUNT(*) > 1)', 'CRITICAL'),
  (7, 'dealers', 'null_dealer_name',        'dealer_name IS NULL',     'WARNING');

-- ── daily_listings ───────────────────────────────────────────────────────────
INSERT INTO {CATALOG}.{TARGET_SCHEMA}.data_quality_checks
VALUES
  (8,  'daily_listings', 'null_transaction_listing_id', 'transaction_listing_id IS NULL', 'CRITICAL'),
  (9,  'daily_listings', 'null_retrieval_date',         'retrieval_date IS NULL',          'CRITICAL'),
  (10, 'daily_listings', 'future_retrieval_date',       'retrieval_date > current_date()', 'WARNING'),
  (11, 'daily_listings', 'null_list_price',             'list_price IS NULL',              'WARNING');
