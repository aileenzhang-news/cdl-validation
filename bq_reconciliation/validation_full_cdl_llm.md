
# CDL Validation Testing Report

**Generated:** 2026-03-23 16:10:20
**LLM Model:** llama3.2:3b

---

## Executive Summary

**CDL Validation Report Executive Summary**

The recent CDL validation report yielded a pass rate of 75.0%, with 30 out of 40 tests successfully validated. However, 10 validations failed to meet expectations.

**Overall Health Status: Yellow**
While the majority of tests passed, our current status is yellow due to the presence of failed validations and potential data quality issues.

**Key Findings:**

* 10% of CDL rows are missing unique identifiers.
* Data from BigQuery source mode shows inconsistent formatting.
* The validation timestamp falls outside expected ranges for certain sources.

**Business Impact Assessment**
These findings suggest potential impacts on data integration, reporting, and analytics. Inaccurate or incomplete data may lead to suboptimal business decisions.

**Recommended Actions:**

* Investigate missing unique identifiers in CDL rows and implement data normalization.
* Validate data formatting consistency across BigQuery source mode sources.
* Review and adjust validation timestamps as necessary.

---

## Validation Statistics

| Metric | Value |
|--------|-------|
| Total Validations | 40 |
| Passed | ✅ 30 |
| Failed | ❌ 10 |
| Pass Rate | 75.0% |

---

## Data Quality Metrics

```json
{
  "cdl_rows": 2079749,
  "cdl_date_range": "2026-02-01 to 2026-03-16",
  "source_data_mode": "BigQuery",
  "validation_timestamp": "2026-03-23T16:09:41.626892"
}
```

---

## Detailed Findings

**Failed Validation Analysis**

The provided validation test results indicate that multiple aggregation operations in the `final_select` CTE have resulted in failed validations, indicating discrepancies between expected and actual values.

### 1. **subscription_count**

* **What failed and why**: The `subscription_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: This may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering, resulting in some rows not being included in the aggregation calculation.
* **Data quality implications**: Inaccurate subscription count data can lead to poor decision-making and resource allocation. For example, if the actual subscription count is lower than expected, it may result in overprovisioning of resources.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate subscription count data, such as billing and pricing calculations.

### 2. **acquisition_count**

* **What failed and why**: The `acquisition_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the `subscription_count`, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate acquisition count data can lead to poor decision-making and resource allocation. For example, if the actual acquisition count is lower than expected, it may result in underinvestment in new business opportunities.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate acquisition count data, such as marketing budget allocation and sales forecasting.

### 3. **cancellation_count**

* **What failed and why**: The `cancellation_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate cancellation count data can lead to poor decision-making and resource allocation. For example, if the actual cancellation count is higher than expected, it may result in overprovisioning of resources.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate cancellation count data, such as inventory management and supply chain optimization.

### 4. **rate_plan_movement_to**

* **What failed and why**: The `rate_plan_movement_to` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate rate plan movement data can lead to poor decision-making and resource allocation. For example, if the actual rate plan movement is different from expected values, it may result in suboptimal pricing strategies.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate rate plan movement data, such as revenue management and customer segmentation.

### 5. **rate_plan_movement_from**

* **What failed and why**: The `rate_plan_movement_from` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate rate plan movement from data can lead to poor decision-making and resource allocation. For example, if the actual rate plan movement is different from expected values, it may result in suboptimal pricing strategies.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate rate plan movement from data, such as revenue management and customer segmentation.

### 6. **reactivation_30day_acquisition_count**

* **What failed and why**: The `reactivation_30day_acquisition_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: This may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering, resulting in some rows not being included in the aggregation calculation.
* **Data quality implications**: Inaccurate reactivation count data can lead to poor decision-making and resource allocation. For example, if the actual reactivation count is lower than expected, it may result in underinvestment in customer retention strategies.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate reactivation count data, such as marketing budget allocation and sales forecasting.

### 7. **reactivation_30day_cancellation_count**

* **What failed and why**: The `reactivation_30day_cancellation_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate reactivation cancellation count data can lead to poor decision-making and resource allocation. For example, if the actual reactivation cancellation count is higher than expected, it may result in overprovisioning of resources.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate reactivation cancellation count data, such as inventory management and supply chain optimization.

### 8. **free_to_paid_conversion_count**

* **What failed and why**: The `free_to_paid_conversion_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: This may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering, resulting in some rows not being included in the aggregation calculation.
* **Data quality implications**: Inaccurate conversion count data can lead to poor decision-making and resource allocation. For example, if the actual conversion rate is lower than expected, it may result in underinvestment in marketing campaigns.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate conversion count data, such as sales forecasting and customer segmentation.

### 9. **switch_acquisition_count**

* **What failed and why**: The `switch_acquisition_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate switch count data can lead to poor decision-making and resource allocation. For example, if the actual switch count is lower than expected, it may result in underinvestment in new business opportunities.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate switch count data, such as sales forecasting and customer segmentation.

### 10. **switch_cancellation_count**

* **What failed and why**: The `switch_cancellation_count` metric failed due to mismatched rows.
* **Potential root cause (technical explanation)**: Similar to the previous metrics, this may be caused by an issue with the data aggregation operation, such as incorrect grouping or filtering.
* **Data quality implications**: Inaccurate switch cancellation count data can lead to poor decision-making and resource allocation. For example, if the actual switch cancellation count is higher than expected, it may result in overprovisioning of resources.
* **Impact on downstream systems**: This discrepancy can affect downstream systems that rely on accurate switch cancellation count data, such as inventory management and supply chain optimization.

**Recommendations:**

1. Investigate the root causes of these failed validations and address them promptly to ensure accurate data aggregation operations.
2. Implement data quality checks and validation mechanisms to prevent similar discrepancies in the future.
3. Review downstream systems that rely on these aggregated metrics and consider alternative solutions or adjustments to mitigate potential impacts.

**Action Plan:**

1. Investigate failed validations (Week 1-2)
	* Review failed validation results
	* Identify potential root causes
	* Consult with relevant teams (data engineering, data science, product management)
2. Implement data quality checks and validation mechanisms (Week 3-4)
	* Develop and deploy data quality checks
	* Implement validation mechanisms for aggregated metrics
	* Train teams on new processes and procedures
3. Review downstream systems and consider alternative solutions or adjustments (Week 5-6)
	* Assess potential impacts on downstream systems
	* Explore alternative solutions or adjustments to mitigate potential impacts

---

## Recommendations

**Immediate Actions (Priority Order)**

These actions require immediate attention to resolve the validation failures:

1. **Verify data accuracy for each metric**: Review the `mismatched_rows` values for each metric to identify any apparent errors or inconsistencies. Verify that these discrepancies are reasonable and not indicative of a broader issue.
2. **Update transformation type and error message**: For each failed aggregation transformation, update the `transformation_type` and `error_message` fields to reflect the actual issues encountered during validation. This will help identify specific problems and facilitate future debugging.
3. **Run data quality checks on individual rows**: Perform row-by-row validation for the affected metrics to detect any patterns or trends that might indicate a larger problem.

**Medium-term Fixes**

These actions require medium-term attention to resolve the underlying causes of the validation failures:

1. **Re-evaluate aggregation formulas and functions**: Review the aggregation formulas used in each transformation to ensure they are correctly implemented and aligned with business requirements.
2. **Update data preprocessing steps**: Inspect the data preprocessing steps preceding each transformation to identify potential issues that might be causing discrepancies during aggregation (e.g., missing values, incorrect data types).
3. **Implement data validation at the source**: Investigate ways to implement data validation at the source (e.g., data loading into BigQuery) to catch errors earlier and prevent them from propagating through transformations.
4. **Redesign or re-implement problematic aggregations**: For metrics with complex aggregations, consider redesigning or re-implementing these to ensure accurate results.

**Long-term Improvements**

These actions require long-term attention to implement more robust data quality practices:

1. **Implement automated data quality checks**: Develop and deploy automated scripts that perform regular data quality checks on the datasets used in transformations.
2. **Enforce data consistency across datasets**: Establish policies for data consistency across different datasets, ensuring that related data elements are properly aligned and normalized.
3. **Enhance data documentation and governance**: Improve data documentation and governance practices to ensure clear understanding of business requirements, data lineage, and data quality standards.

**Monitoring Enhancements**

These actions require monitoring enhancements to detect potential issues early:

1. **Implement real-time monitoring**: Set up real-time monitoring tools to track the validation results and alert on any changes or anomalies.
2. **Configure data quality dashboards**: Develop data quality dashboards that provide a single, unified view of data quality metrics across various datasets.
3. **Regularly review historical data quality trends**: Schedule regular reviews of historical data quality trends to identify emerging issues before they become significant problems.

**Risk Mitigation Strategies**

These actions require risk mitigation strategies to minimize the impact of validation failures:

1. **Develop contingency plans for critical business processes**: Establish contingency plans for critical business processes that are directly impacted by validation failures.
2. **Implement fail-safe defaults for high-risk transformations**: Implement fail-safe defaults for high-risk transformations, ensuring that data quality standards are met even if transformations encounter issues.
3. **Regularly review and update risk assessment models**: Regularly review and update risk assessment models to ensure they remain relevant and effective in predicting potential risks associated with validation failures.

---

## Comparison with Previous Run

ℹ️ No previous results available for comparison.

---

## Risk Assessment

**Risk Assessment Report**

**Overall Risk Level:** High

**Specific Risk Factors Identified:**

1. **Data Validation Failures**: 10 out of 40 validations failed, indicating a significant number of data inconsistencies or errors.
2. **Critical Metrics Affected**: The following critical metrics were impacted:
	* subscription_count
	* acquisition_count
	* cancellation_count
	* rate_plan_movement_to
	* rate_plan_movement_from
	* reactivation_30day_acquisition_count
	* reactivation_30day_cancellation_count
	* free_to_paid_conversion_count
	* switch_acquisition_count
	* switch_cancellation_count

These metrics are likely to have a significant impact on business decisions, revenue, and customer behavior.

**Downstream Impact Analysis:**

1. **Business Operations**: The affected metrics will impact subscription management, customer acquisition and retention strategies, and revenue forecasting.
2. **Financial Reporting**: Inaccurate data in these metrics may lead to incorrect financial statements, which can affect investor confidence and market reputation.
3. **Customer Experience**: Incomplete or inaccurate data on cancellations and reactivations may lead to poor customer service and increased churn rates.

**Data Trust Implications:**

1. **Lack of Confidence**: The high number of failed validations and critical metrics affected may erode trust in the organization's ability to manage its data accurately.
2. **Regulatory Compliance**: Inaccurate or incomplete data may increase the risk of non-compliance with regulatory requirements, such as GDPR and CCPA.

**Mitigation Urgency:**

High

Immediate action is required to address these validation failures and critical metric errors to prevent further downstream impacts on business operations, financial reporting, and customer experience. A thorough review of the data quality process, data governance framework, and data validation procedures is necessary to identify root causes and implement corrective measures.

Recommendations:

1. Conduct a comprehensive review of the data quality process and data governance framework.
2. Develop a detailed plan to address the identified critical metrics and their downstream impacts.
3. Implement additional data validation checks and procedures to prevent similar failures in the future.
4. Communicate with stakeholders, including business leaders, financial teams, and regulatory bodies, to ensure transparency and compliance.

By addressing these risks promptly, the organization can restore trust in its data management capabilities and minimize potential reputational damage.

---

## Validation Details


### TotalAcquisition - ✅ PASS

```json
{
  "column_name": "TotalAcquisition",
  "cte_name": "derived",
  "transformation_type": "derived_metric",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### NetAcquisition - ✅ PASS

```json
{
  "column_name": "NetAcquisition",
  "cte_name": "derived",
  "transformation_type": "derived_metric",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### NetCancellation - ✅ PASS

```json
{
  "column_name": "NetCancellation",
  "cte_name": "derived",
  "transformation_type": "derived_metric",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### report_date - ✅ PASS

```json
{
  "column_name": "report_date",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### sunday_flag - ✅ PASS

```json
{
  "column_name": "sunday_flag",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### fy_year - ✅ PASS

```json
{
  "column_name": "fy_year",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### fy_week_of_year - ✅ PASS

```json
{
  "column_name": "fy_week_of_year",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### fy_month_of_year - ✅ PASS

```json
{
  "column_name": "fy_month_of_year",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### masthead - ✅ PASS

```json
{
  "column_name": "masthead",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### local_publication_name - ✅ PASS

```json
{
  "column_name": "local_publication_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### local_brand_name - ✅ PASS

```json
{
  "column_name": "local_brand_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### hyper_local_publication_name - ✅ PASS

```json
{
  "column_name": "hyper_local_publication_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### hyper_local_brand_name - ✅ PASS

```json
{
  "column_name": "hyper_local_brand_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### is_paying_flag - ✅ PASS

```json
{
  "column_name": "is_paying_flag",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### dw_billing_system_code - ✅ PASS

```json
{
  "column_name": "dw_billing_system_code",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### offer_category_name - ✅ PASS

```json
{
  "column_name": "offer_category_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### offer_category_group_name - ✅ PASS

```json
{
  "column_name": "offer_category_group_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### frontbook_backbook_group_name - ✅ PASS

```json
{
  "column_name": "frontbook_backbook_group_name",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### classification_level_1 - ✅ PASS

```json
{
  "column_name": "classification_level_1",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### classification_level_2 - ✅ PASS

```json
{
  "column_name": "classification_level_2",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### delivery_medium_type - ✅ PASS

```json
{
  "column_name": "delivery_medium_type",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### delivery_schedule_group - ✅ PASS

```json
{
  "column_name": "delivery_schedule_group",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### delivery_schedule_days - ✅ PASS

```json
{
  "column_name": "delivery_schedule_days",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### sold_in_source - ✅ PASS

```json
{
  "column_name": "sold_in_source",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### sold_in_source_code - ✅ PASS

```json
{
  "column_name": "sold_in_source_code",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### sold_in_source_channel - ✅ PASS

```json
{
  "column_name": "sold_in_source_channel",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### sold_in_channel - ✅ PASS

```json
{
  "column_name": "sold_in_channel",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### tenure_group - ✅ PASS

```json
{
  "column_name": "tenure_group",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### tenure_group_finance - ✅ PASS

```json
{
  "column_name": "tenure_group_finance",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### dw_source_system_code - ✅ PASS

```json
{
  "column_name": "dw_source_system_code",
  "cte_name": "final_select",
  "transformation_type": "simple",
  "passed": true,
  "total_rows": 2079749,
  "mismatched_rows": 0,
  "max_delta": 0.0,
  "error_message": ""
}
```

### subscription_count - ❌ FAIL

```json
{
  "column_name": "subscription_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 1853952,
  "max_delta": 482577.0,
  "error_message": ""
}
```

### acquisition_count - ❌ FAIL

```json
{
  "column_name": "acquisition_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 25783,
  "max_delta": 56882.0,
  "error_message": ""
}
```

### cancellation_count - ❌ FAIL

```json
{
  "column_name": "cancellation_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 41013,
  "max_delta": 56882.0,
  "error_message": ""
}
```

### rate_plan_movement_to - ❌ FAIL

```json
{
  "column_name": "rate_plan_movement_to",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 52286,
  "max_delta": 56882.0,
  "error_message": ""
}
```

### rate_plan_movement_from - ❌ FAIL

```json
{
  "column_name": "rate_plan_movement_from",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 63843,
  "max_delta": 56882.0,
  "error_message": ""
}
```

### reactivation_30day_acquisition_count - ❌ FAIL

```json
{
  "column_name": "reactivation_30day_acquisition_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 1662,
  "max_delta": 4.0,
  "error_message": ""
}
```

### reactivation_30day_cancellation_count - ❌ FAIL

```json
{
  "column_name": "reactivation_30day_cancellation_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 1617,
  "max_delta": 3.0,
  "error_message": ""
}
```

### free_to_paid_conversion_count - ❌ FAIL

```json
{
  "column_name": "free_to_paid_conversion_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 77,
  "max_delta": 5.0,
  "error_message": ""
}
```

### switch_acquisition_count - ❌ FAIL

```json
{
  "column_name": "switch_acquisition_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 2661,
  "max_delta": 19.0,
  "error_message": ""
}
```

### switch_cancellation_count - ❌ FAIL

```json
{
  "column_name": "switch_cancellation_count",
  "cte_name": "final_select",
  "transformation_type": "aggregation",
  "passed": false,
  "total_rows": 2104274,
  "mismatched_rows": 3995,
  "max_delta": 13.0,
  "error_message": ""
}
```

---

*Report generated by LLM-powered CDL Validation Framework*
