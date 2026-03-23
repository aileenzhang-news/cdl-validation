-- ============================================================================
-- VALIDATED EXPORT QUERIES FOR CDL SOURCE VALIDATION
-- Generated from mapping.xlsx - all columns verified
-- ============================================================================

-- ============================================================================
-- FILE 1: source_subscription_base.csv
-- Export from: ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct
-- Save to: CDL/bq_reconciliation/source_data/source_subscription_base.csv
-- ============================================================================

SELECT
  -- All 28 columns referenced in mapping.xlsx from sbf table
  classification_level_1,
  classification_level_2,
  delivery_medium_type,
  delivery_schedule_days,
  delivery_schedule_group,
  dw_billing_system_code,
  dw_effective_end_datetime,
  dw_effective_start_datetime,
  dw_publication_id,
  dw_rate_plan_id,
  dw_source_system_code,
  frontbook_backbook_group_name,
  hyper_local_brand_name,
  hyper_local_publication_name,
  is_paying_flag,
  local_brand_name,
  local_publication_name,
  masthead,
  offer_category_group_name,
  offer_category_name,
  ratecard_price,
  sold_in_channel,
  sold_in_source,
  sold_in_source_channel,
  sold_in_source_code,
  subscriber_has_email_flag,
  subscription_count,
  subscription_tenure_start_datetime
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct`
WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '2000-03-30' AND '2026-03-16'
   OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '2000-03-30' AND '2026-03-16';


-- ============================================================================
-- FILE 2: source_subscription_movement.csv
-- Export from: ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct
-- Save to: CDL/bq_reconciliation/source_data/source_subscription_movement.csv
-- ============================================================================

SELECT
  -- All 34 columns referenced in mapping.xlsx from smf table
  classification_level_1,
  classification_level_2,
  delivery_medium_type,
  delivery_schedule_days,
  delivery_schedule_group,
  dw_billing_system_code,
  dw_publication_id,
  dw_rate_plan_id,
  dw_source_system_code,
  frontbook_backbook_group_name,
  fy_month_of_year,
  fy_week_of_year,
  fy_year,
  hyper_local_brand_name,
  hyper_local_publication_name,
  incremental_subscription_movement_count,
  is_paying_flag,
  local_brand_name,
  local_publication_name,
  masthead,
  movement_datetime,
  movement_type_code,
  offer_category_group_name,
  offer_category_name,
  report_date,
  sold_in_channel,
  sold_in_source,
  sold_in_source_channel,
  sold_in_source_code,
  subscriber_has_email_flag,
  subscription_movement_count,
  subscription_movement_count_sub_type,
  subscription_movement_count_type,
  subscription_tenure_start_datetime
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE report_date BETWEEN '2000-03-30' AND '2026-03-16';


-- ============================================================================
-- FILE 3: source_calendar.csv
-- Export from: ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim
-- Save to: CDL/bq_reconciliation/source_data/source_calendar.csv
-- ============================================================================

SELECT
  -- All 5 columns referenced in mapping.xlsx from cal table
  -- Note: Renaming columns to match mapping.xlsx expectations
  fy_month_of_year,
  fy_week_of_year,
  fy_year,
  last_day_of_week AS last_day_of_week_date,  -- Renamed to match mapping
  calendar_date AS report_date                 -- Renamed to match mapping
FROM `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim`
WHERE calendar_date BETWEEN '2000-03-30' AND '2026-03-16';


-- ============================================================================
-- FILE 4: source_schemas.csv
-- Export table schemas for validation
-- Save to: CDL/bq_reconciliation/source_data/source_schemas.csv
-- ============================================================================

SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM `ncau-data-newsquery-prd.bdm_subscription.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('subscription_base_extended_fct', 'subscription_movement_extended_fct')

UNION ALL

SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM `ncau-data-newsquery-prd.prstn_consumer.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'v_calendar_dim';


-- ============================================================================
-- OPTIONAL: DATA VOLUME CHECK (run this first to estimate export size)
-- ============================================================================

-- Check row counts for subscription_base_extended_fct
SELECT
  'subscription_base_extended_fct' as table_name,
  COUNT(*) as row_count,
  MIN(CAST(dw_effective_start_datetime AS DATE)) as min_date,
  MAX(CAST(dw_effective_start_datetime AS DATE)) as max_date
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct`
WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '2000-03-30' AND '2026-03-16'
   OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '2000-03-30' AND '2026-03-16'

UNION ALL

-- Check row counts for subscription_movement_extended_fct
SELECT
  'subscription_movement_extended_fct' as table_name,
  COUNT(*) as row_count,
  MIN(CAST(report_date AS DATE)) as min_date,
  MAX(CAST(report_date AS DATE)) as max_date
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE CAST(report_date AS DATE) BETWEEN '2000-03-30' AND '2026-03-16'

UNION ALL

-- Check row counts for v_calendar_dim
SELECT
  'v_calendar_dim' as table_name,
  COUNT(*) as row_count,
  MIN(CAST(report_date AS DATE)) as min_date,
  MAX(CAST(report_date AS DATE)) as max_date
FROM `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim`
WHERE CAST(report_date AS DATE) BETWEEN '2000-03-30' AND '2026-03-16';
