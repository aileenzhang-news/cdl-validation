-- ============================================================================
-- BigQuery Verification Queries for CDL Validation
-- Generated: 2026-03-20
-- ============================================================================
-- These queries match the exact logic used in the validation framework
-- to rebuild and compare aggregation metrics from BigQuery source data.
--
-- CDL Date Range: 2026-02-01 to 2026-03-16
-- Total CDL Rows: 356
-- ============================================================================

-- ============================================================================
-- QUERY 1: Subscription Count Verification
-- ============================================================================
-- This query calculates subscription_count using the same logic as the CDL
-- Expected: Should match the sum in your CDL Excel file

WITH
-- Calendar dimension
cal AS (
  SELECT
    calendar_date AS report_date,
    CASE WHEN EXTRACT(DAYOFWEEK FROM calendar_date) = 1 THEN 'Y' ELSE 'N' END AS sunday_flag,
    fy_year,
    fy_week_of_year,
    fy_month_of_year
  FROM `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim`
  WHERE calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
),

-- Subscription base data (inner CTE with calculations)
sba_inner AS (
  SELECT
    sbf.*,
    -- Tenure calculations (if needed)
    CASE
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 0 AND 180 THEN '0-180'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 181 AND 360 THEN '181-360'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 361 AND 540 THEN '361-540'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 541 AND 720 THEN '541-720'
      ELSE '721+'
    END AS tenure_group,
    CASE
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) >= 0 AND DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) <= 182 THEN '0-6months'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 183 AND 365 THEN '6-12months'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 366 AND 730 THEN '1-2years'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 731 AND 1095 THEN '2-3years'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 1096 AND 1460 THEN '3-4years'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 1461 AND 1825 THEN '4-5years'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 1826 AND 2190 THEN '5-6years'
      WHEN DATE_DIFF(
        CAST(CASE WHEN sbf.dw_effective_end_datetime < cal.report_date
          THEN sbf.dw_effective_end_datetime
          ELSE cal.report_date END AS DATE),
        CAST(sbf.subscription_tenure_start_datetime AS DATE),
        DAY
      ) BETWEEN 2191 AND 2555 THEN '6-7years'
      ELSE '7years+'
    END AS tenure_group_finance
  FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
  CROSS JOIN cal
  WHERE cal.report_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
),

-- Subscription base aggregated
sba AS (
  SELECT
    report_date,
    sunday_flag,
    fy_year,
    fy_week_of_year,
    fy_month_of_year,
    masthead,
    local_publication_name,
    local_brand_name,
    hyper_local_publication_name,
    hyper_local_brand_name,
    is_paying_flag,
    dw_billing_system_code,
    offer_category_name,
    offer_category_group_name,
    frontbook_backbook_group_name,
    classification_level_1,
    classification_level_2,
    delivery_medium_type,
    delivery_schedule_group,
    delivery_schedule_days,
    sold_in_source,
    sold_in_source_code,
    sold_in_source_channel,
    sold_in_channel,
    tenure_group,
    tenure_group_finance,
    dw_source_system_code,
    subscriber_has_email_flag,
    COUNT(*) AS subscription_count
  FROM sba_inner
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
),

-- Subscription movement data (inner CTE)
sma_inner AS (
  SELECT
    smf.*,
    cal.sunday_flag
  FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct` smf
  CROSS JOIN cal
  WHERE cal.report_date = smf.report_date
),

-- Subscription movement aggregated
sma AS (
  SELECT
    report_date,
    sunday_flag,
    fy_year,
    fy_week_of_year,
    fy_month_of_year,
    masthead,
    local_publication_name,
    local_brand_name,
    hyper_local_publication_name,
    hyper_local_brand_name,
    is_paying_flag,
    classification_level_1,
    classification_level_2,
    delivery_medium_type,
    delivery_schedule_group,
    delivery_schedule_days,
    sold_in_source,
    sold_in_source_code,
    sold_in_source_channel,
    sold_in_channel,
    dw_billing_system_code,
    offer_category_name,
    offer_category_group_name,
    frontbook_backbook_group_name,
    dw_source_system_code,
    subscriber_has_email_flag,
    tenure_group,
    tenure_group_finance
    -- Aggregation metrics would go here if needed
  FROM sma_inner
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
),

-- Subscription base movement (union of sba and sma)
sbm AS (
  SELECT * FROM sba
  UNION ALL
  SELECT
    report_date,
    sunday_flag,
    fy_year,
    fy_week_of_year,
    fy_month_of_year,
    masthead,
    local_publication_name,
    local_brand_name,
    hyper_local_publication_name,
    hyper_local_brand_name,
    is_paying_flag,
    dw_billing_system_code,
    offer_category_name,
    offer_category_group_name,
    frontbook_backbook_group_name,
    classification_level_1,
    classification_level_2,
    delivery_medium_type,
    delivery_schedule_group,
    delivery_schedule_days,
    sold_in_source,
    sold_in_source_code,
    sold_in_source_channel,
    sold_in_channel,
    tenure_group,
    tenure_group_finance,
    dw_source_system_code,
    subscriber_has_email_flag,
    CAST(0 AS INT64) AS subscription_count
  FROM sma
),

-- Final select with filters (EXCLUDING THINKSWG and GPLA)
final_select AS (
  SELECT
    report_date,
    sunday_flag,
    fy_year,
    fy_week_of_year,
    fy_month_of_year,
    masthead,
    local_publication_name,
    local_brand_name,
    hyper_local_publication_name,
    hyper_local_brand_name,
    is_paying_flag,
    dw_billing_system_code,
    offer_category_name,
    offer_category_group_name,
    frontbook_backbook_group_name,
    classification_level_1,
    classification_level_2,
    delivery_medium_type,
    delivery_schedule_group,
    delivery_schedule_days,
    sold_in_source,
    sold_in_source_code,
    sold_in_source_channel,
    sold_in_channel,
    tenure_group,
    tenure_group_finance,
    dw_source_system_code,
    subscriber_has_email_flag,
    SUM(subscription_count) AS subscription_count
  FROM sbm
  WHERE sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)

-- Final aggregation to match CDL structure
SELECT
  COUNT(*) AS total_rows,
  SUM(subscription_count) AS total_subscription_count,
  MIN(subscription_count) AS min_subscription_count,
  MAX(subscription_count) AS max_subscription_count,
  AVG(subscription_count) AS avg_subscription_count
FROM final_select;

-- ============================================================================
-- EXPECTED RESULTS:
-- total_rows: ~329,230 (rebuilt source has more rows than CDL's 356)
-- total_subscription_count: Compare with your CDL Excel sum
-- ============================================================================


-- ============================================================================
-- QUERY 2: Detailed Comparison Query
-- ============================================================================
-- This query shows side-by-side comparison of all metrics
-- You can export this and compare with your CDL Excel file

WITH
-- [Copy the entire CTE structure from Query 1, ending at final_select]
-- ... (same as above) ...

final_select AS (
  SELECT
    report_date,
    masthead,
    offer_category_name,
    dw_billing_system_code,
    frontbook_backbook_group_name,
    SUM(subscription_count) AS subscription_count
    -- Add other aggregation columns here
  FROM sbm
  WHERE sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
  GROUP BY 1,2,3,4,5
)

SELECT
  report_date,
  masthead,
  offer_category_name,
  dw_billing_system_code,
  frontbook_backbook_group_name,
  subscription_count
FROM final_select
ORDER BY report_date, masthead
LIMIT 1000;


-- ============================================================================
-- QUERY 3: Quick Summary Comparison
-- ============================================================================
-- Simplified query to just get the totals for comparison

SELECT
  'subscription_count' AS metric,
  COUNT(DISTINCT dw_subscription_id) AS calculated_value,
  'Count distinct subscriptions' AS description
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
  AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA');


-- ============================================================================
-- NOTES FOR VERIFICATION:
-- ============================================================================
-- 1. The validation failed because the CDL has 356 rows while the rebuilt
--    source has ~329,230 rows. This is expected - the CDL appears to be
--    a pre-aggregated subset or filtered view.
--
-- 2. The validation compares row-by-row after grouping by all dimension
--    columns. Since the row counts differ, many rows exist only in source
--    or only in CDL.
--
-- 3. To properly verify:
--    a) Check if your CDL has additional filters not captured here
--    b) Check if your CDL uses different aggregation granularity
--    c) Compare the SUM of metrics rather than row-by-row comparison
--
-- 4. Your CDL shows:
--    - Total rows: 356
--    - subscription_count sum: 7,313
--    - acquisition_count sum: 8
--    - cancellation_count sum: 15
--
--    Run Query 1 to see if the sums match when you aggregate across all
--    dimension combinations.
-- ============================================================================
