-- ============================================================================
-- SIMPLE BigQuery Verification Queries
-- ============================================================================
-- These are simplified queries you can run directly in BigQuery
-- to verify your CDL metrics without the complex CTE structure.
--
-- Date Range: 2026-02-01 to 2026-03-16 (your CDL period)
-- ============================================================================

-- ============================================================================
-- QUERY 1: Total Subscription Count
-- ============================================================================
-- This counts how many subscriptions existed during your date range
-- Compare the result with SUM(subscription_count) in your CDL Excel

SELECT
  COUNT(*) AS total_subscription_count_rows,
  COUNT(DISTINCT dw_subscription_id) AS unique_subscriptions
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
  AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA');

-- ============================================================================
-- Expected:
-- If your CDL shows subscription_count sum = 7,313, check if this matches
-- ============================================================================


-- ============================================================================
-- QUERY 2: Subscription Count by Date
-- ============================================================================
-- This breaks down subscription count by calendar_date
-- You can compare specific dates with your CDL Excel

SELECT
  cal.calendar_date,
  COUNT(*) AS subscription_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
  AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
GROUP BY cal.calendar_date
ORDER BY cal.calendar_date;


-- ============================================================================
-- QUERY 3: Subscription Count by Masthead (Top 10)
-- ============================================================================
-- This shows subscription count by masthead
-- Compare with your CDL to see if the mastheads match

SELECT
  sbf.masthead,
  COUNT(*) AS subscription_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
  AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
GROUP BY sbf.masthead
ORDER BY subscription_count DESC
LIMIT 10;


-- ============================================================================
-- QUERY 4: Acquisition Count
-- ============================================================================
-- This counts acquisition movements in your date range
-- Compare with SUM(acquisition_count) in your CDL Excel

SELECT
  COUNT(*) AS acquisition_movement_rows,
  SUM(CASE WHEN movement_type = 'acquisition' THEN 1 ELSE 0 END) AS acquisition_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE report_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND sold_in_source_code NOT IN ('THINKSWG', 'GPLA');

-- ============================================================================
-- Expected: Your CDL shows acquisition_count sum = 8
-- ============================================================================


-- ============================================================================
-- QUERY 5: Cancellation Count
-- ============================================================================
-- This counts cancellation movements in your date range
-- Compare with SUM(cancellation_count) in your CDL Excel

SELECT
  COUNT(*) AS cancellation_movement_rows,
  SUM(CASE WHEN movement_type = 'cancellation' THEN 1 ELSE 0 END) AS cancellation_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE report_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND sold_in_source_code NOT IN ('THINKSWG', 'GPLA');

-- ============================================================================
-- Expected: Your CDL shows cancellation_count sum = 15
-- ============================================================================


-- ============================================================================
-- QUERY 6: All Movement Types Summary
-- ============================================================================
-- This shows counts for all movement types
-- Useful to understand what movements occurred in your date range

SELECT
  movement_type,
  COUNT(*) AS movement_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE report_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
GROUP BY movement_type
ORDER BY movement_count DESC;


-- ============================================================================
-- QUERY 7: Detailed Movement Breakdown
-- ============================================================================
-- This shows all the movement counts matching your CDL columns

SELECT
  COUNT(*) AS total_movement_rows,
  SUM(CASE WHEN movement_type = 'acquisition' THEN 1 ELSE 0 END) AS acquisition_count,
  SUM(CASE WHEN movement_type = 'cancellation' THEN 1 ELSE 0 END) AS cancellation_count,
  SUM(CASE WHEN movement_type LIKE '%reactivation%' AND movement_detail LIKE '%30%' THEN 1 ELSE 0 END) AS reactivation_30day_acquisition_count,
  SUM(CASE WHEN movement_type LIKE '%free_to_paid%' THEN 1 ELSE 0 END) AS free_to_paid_conversion_count,
  SUM(CASE WHEN movement_type = 'switch' AND net_acquisition_flag = 'Y' THEN 1 ELSE 0 END) AS switch_acquisition_count,
  SUM(CASE WHEN movement_type = 'switch' AND net_acquisition_flag = 'N' THEN 1 ELSE 0 END) AS switch_cancellation_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE report_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND sold_in_source_code NOT IN ('THINKSWG', 'GPLA');

-- ============================================================================
-- Compare these totals with your CDL Excel sums:
-- - acquisition_count: 8
-- - cancellation_count: 15
-- - Other counts as shown in your CDL
-- ============================================================================


-- ============================================================================
-- QUERY 8: Sample Rows with All Dimensions
-- ============================================================================
-- This query shows a sample of how the data looks when grouped by all
-- dimensions (matching your CDL structure). Limit to 100 rows for review.

SELECT
  cal.calendar_date,
  sbf.masthead,
  sbf.offer_category_name,
  sbf.dw_billing_system_code,
  sbf.frontbook_backbook_group_name,
  sbf.classification_level_1,
  sbf.classification_level_2,
  COUNT(*) AS subscription_count
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
  AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
  AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
GROUP BY 1,2,3,4,5,6,7
ORDER BY subscription_count DESC
LIMIT 100;


-- ============================================================================
-- QUERY 9: Compare Your CDL Row Count
-- ============================================================================
-- This shows how many unique dimension combinations exist in the source
-- Your CDL has 356 rows, so this helps explain the difference

SELECT
  COUNT(*) AS unique_dimension_combinations
FROM (
  SELECT DISTINCT
    cal.calendar_date,
    sbf.masthead,
    sbf.offer_category_name,
    sbf.dw_billing_system_code,
    sbf.frontbook_backbook_group_name,
    sbf.classification_level_1,
    sbf.classification_level_2,
    sbf.delivery_medium_type,
    sbf.tenure_group -- If you need tenure calculations
  FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct` sbf
  CROSS JOIN `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim` cal
  WHERE cal.calendar_date BETWEEN '2026-02-01' AND '2026-03-16'
    AND cal.calendar_date BETWEEN sbf.dw_effective_start_datetime AND sbf.dw_effective_end_datetime
    AND sbf.sold_in_source_code NOT IN ('THINKSWG', 'GPLA')
);

-- ============================================================================
-- Expected: This will show ~329,000+ combinations
-- Your CDL has only 356 rows, which suggests:
-- 1. Your CDL might have additional aggregation or filters
-- 2. Your CDL might be a different granularity (e.g., aggregated to weekly)
-- 3. Your CDL might have different selection criteria
-- ============================================================================


-- ============================================================================
-- RECOMMENDED VERIFICATION STEPS:
-- ============================================================================
-- 1. Run Query 1 to check total subscription count
--    Compare with SUM(subscription_count) in your CDL
--
-- 2. Run Query 4 and 5 to verify acquisition and cancellation counts
--    Compare with SUM(acquisition_count) and SUM(cancellation_count)
--
-- 3. If totals match, the validation "failures" are likely due to:
--    - Different row-level granularity (CDL has 356 rows vs source ~329K)
--    - Different grouping logic
--    - Additional transformations in your CDL creation process
--
-- 4. If totals DON'T match, investigate:
--    - Date range filters (check your CDL's actual date range)
--    - Additional filters (besides THINKSWG/GPLA exclusion)
--    - Movement type definitions
-- ============================================================================
