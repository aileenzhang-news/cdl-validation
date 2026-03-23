-- ============================================================================
-- QUICK TEST: 2026年2-3月数据导出查询
-- 用于快速测试validation pipeline是否正常工作
-- ============================================================================

-- ============================================================================
-- FILE 1: source_subscription_base_2026_feb_mar.csv
-- Save to: CDL/bq_reconciliation/source_data_test/
-- ============================================================================

SELECT
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
WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '2026-02-01' AND '2026-03-31'
   OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '2026-02-01' AND '2026-03-31';


-- ============================================================================
-- FILE 2: source_subscription_movement_2026_feb_mar.csv
-- Save to: CDL/bq_reconciliation/source_data_test/
-- ============================================================================

SELECT
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
WHERE CAST(report_date AS DATE) BETWEEN '2026-02-01' AND '2026-03-31';


-- ============================================================================
-- FILE 3: source_calendar_2026_feb_mar.csv
-- Save to: CDL/bq_reconciliation/source_data_test/
-- ============================================================================

SELECT
  fy_month_of_year,
  fy_week_of_year,
  fy_year,
  last_day_of_week AS last_day_of_week_date,
  calendar_date AS report_date
FROM `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim`
WHERE calendar_date BETWEEN '2026-02-01' AND '2026-03-31';


-- ============================================================================
-- VOLUME CHECK: 预估导出数据量
-- ============================================================================

SELECT
  'subscription_base_extended_fct' as table_name,
  COUNT(*) as row_count,
  MIN(CAST(dw_effective_start_datetime AS DATE)) as min_date,
  MAX(CAST(dw_effective_start_datetime AS DATE)) as max_date
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_base_extended_fct`
WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '2026-02-01' AND '2026-03-31'
   OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '2026-02-01' AND '2026-03-31'

UNION ALL

SELECT
  'subscription_movement_extended_fct' as table_name,
  COUNT(*) as row_count,
  MIN(CAST(report_date AS DATE)) as min_date,
  MAX(CAST(report_date AS DATE)) as max_date
FROM `ncau-data-newsquery-prd.bdm_subscription.subscription_movement_extended_fct`
WHERE CAST(report_date AS DATE) BETWEEN '2026-02-01' AND '2026-03-31'

UNION ALL

SELECT
  'v_calendar_dim' as table_name,
  COUNT(*) as row_count,
  MIN(calendar_date) as min_date,
  MAX(calendar_date) as max_date
FROM `ncau-data-newsquery-prd.prstn_consumer.v_calendar_dim`
WHERE calendar_date BETWEEN '2026-02-01' AND '2026-03-31';


-- ============================================================================
-- 使用说明
-- ============================================================================

/*
1. 创建测试目录:
   mkdir -p /Users/aileen.zhang/Documents/CDL/bq_reconciliation/source_data_test

2. 在BigQuery Console运行上面3个查询

3. 保存CSV文件:
   - source_subscription_base_2026_feb_mar.csv
   - source_subscription_movement_2026_feb_mar.csv
   - source_calendar_2026_feb_mar.csv

4. 同时也导出schemas (只需要一次):
   cp source_data/source_schemas.csv source_data_test/

5. 测试validation:
   python3 validate_from_source.py \
     --source-data source_data_test \
     --validate-all

预期结果:
- 派生指标验证: ✅ 100% pass
- 聚合指标验证: 应该大幅改善（如果时间范围匹配的话）
*/
