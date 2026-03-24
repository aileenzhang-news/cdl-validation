# CDL Migration Acceptance Validator

Validates that CDL can safely replace PRSTN as the source of truth for subscription data by comparing query results between the two tables.

## What This Tool Does

Runs business validation queries on both **CDL** and **PRSTN** tables in BigQuery and verifies that all results match.

- **CDL (New Layer)**: `ncau-data-newsquery-sit.cdl.subscription_transaction_fct`
- **PRSTN (Old Layer)**: `ncau-data-newsquery-prd.prstn_consumer.subscription_base_movement_agg_snap`

**Validation Method:**
- Query Result Matching - Business queries run on both tables and results are compared

**Acceptance Criteria:** All critical queries must pass.

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Authenticate to BigQuery

```bash
gcloud auth application-default login
```

Or use a service account:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

---

## Usage

### Basic Validation (Full Data)

```bash
python3 validate_migration_acceptance.py
```

This will:
- Connect to BigQuery using default credentials
- Run validation queries on full dataset
- Generate `cdl_migration_acceptance_report.json`
- Exit with code 0 if validation passes, 1 if it fails

### Quick Testing with Date Filter (Recommended)

For faster testing, filter to a specific date range:

```bash
# Test on February 2026 data only (fast)
python3 validate_migration_acceptance.py \
  --date-start 2026-02-01 \
  --date-end 2026-02-28

# Test on single day (very fast)
python3 validate_migration_acceptance.py \
  --date-start 2026-03-01 \
  --date-end 2026-03-01
```

**Benefits:**
- Queries run 10-100x faster on partitioned data
- Lower BigQuery costs
- Faster iteration during development
- Remove date filters for final full validation

### Custom Options

```bash
# Use different tables
python3 validate_migration_acceptance.py \
  --cdl-table "project.dataset.cdl_table" \
  --prstn-table "project.dataset.prstn_table"

# Adjust tolerance (default: 0.01)
python3 validate_migration_acceptance.py --tolerance 0.001

# Use custom queries file
python3 validate_migration_acceptance.py \
  --queries-file my_queries.json

# Use service account
python3 validate_migration_acceptance.py \
  --credentials /path/to/service-account.json

# Combine options
python3 validate_migration_acceptance.py \
  --date-start 2026-02-01 \
  --date-end 2026-02-28 \
  --tolerance 0.001 \
  --output feb_validation_report
```

---

## Validation Queries

The tool runs 5 critical business queries in `cdl_migration_acceptance_queries.json`:

1. **total_subscription_count** - Validates total subscription volume matches
2. **total_acquisition_cancellation** - Validates acquisition and cancellation totals
3. **subscription_by_date** - Validates daily subscription counts (temporal check)
4. **subscription_by_masthead** - Validates subscription counts by publication
5. **row_count_and_date_range** - Validates data completeness (row count and date coverage)

### Custom Queries

Create a JSON file with your queries:

```json
[
  {
    "name": "my_custom_check",
    "description": "Description of what this checks",
    "sql": "SELECT COUNT(*) as total FROM `{table}` WHERE condition = true",
    "critical": true
  }
]
```

**Important:**
- Use `{table}` as a placeholder - it will be replaced with CDL and PRSTN table paths
- Date filters (if specified) are automatically injected into queries
- All queries must use `report_date` column for date filtering to work

### How Date Filtering Works

When you specify `--date-start` and `--date-end`, the tool automatically injects WHERE clauses:

**Original Query:**
```sql
SELECT report_date, SUM(subscription_count) as total
FROM `{table}`
GROUP BY report_date
ORDER BY report_date
```

**With `--date-start 2026-02-01 --date-end 2026-02-28`:**
```sql
SELECT report_date, SUM(subscription_count) as total
FROM `{table}`
WHERE report_date >= '2026-02-01' AND report_date <= '2026-02-28'
GROUP BY report_date
ORDER BY report_date
```

This filters data before aggregation, making queries much faster on large partitioned tables.

---

## Output

### Console Output

```
================================================================================
CDL MIGRATION ACCEPTANCE VALIDATION
================================================================================

🆕 NEW LAYER (CDL): ncau-data-newsquery-sit.cdl.subscription_transaction_fct
📦 OLD LAYER (PRSTN): ncau-data-newsquery-prd.prstn_consumer.subscription_base_movement_agg_snap

================================================================================
BUSINESS QUERY VALIDATION
================================================================================
  [1/5] total_subscription_count
    ✓ PASS
  [2/5] total_acquisition_cancellation
    ✓ PASS
  ...

================================================================================
MIGRATION ACCEPTANCE DECISION
================================================================================

🎉 ✅ MIGRATION ACCEPTED ✅ 🎉

CDL produces equivalent results to PRSTN for all critical business queries.
CDL can SAFELY REPLACE PRSTN as the source of truth for subscription data.
```

### JSON Report

Generated as `cdl_migration_acceptance_report.json`:

```json
{
  "migration_accepted": true,
  "timestamp": "2026-03-24T16:00:00",
  "cdl_table": "ncau-data-newsquery-sit.cdl.subscription_transaction_fct",
  "prstn_table": "ncau-data-newsquery-prd.prstn_consumer.subscription_base_movement_agg_snap",
  "critical_queries": {
    "total": 5,
    "passed": 5,
    "failed": 0,
    "pass_rate": 100.0
  },
  "failed_queries": []
}
```

---

## Exit Codes

- `0` - ✅ Migration accepted (all critical queries passed)
- `1` - ❌ Migration not accepted (one or more critical queries failed)

Use in scripts:
```bash
python3 validate_migration_acceptance.py
if [ $? -eq 0 ]; then
    echo "✅ CDL ready to replace PRSTN"
else
    echo "❌ CDL not ready - fix issues first"
fi
```

---

## Troubleshooting

### Authentication Error

**Problem:** `google.auth.exceptions.DefaultCredentialsError`

**Solution:**
```bash
gcloud auth application-default login
```

### Query Failed

**Problem:** A query fails with mismatches

**Steps:**
1. Check the error details in the output
2. Run the failing query manually in BigQuery console on both tables
3. Investigate the data differences
4. Fix CDL data or transformation logic
5. Re-run validation

---

## Project Structure

```
bq_reconciliation/
├── README.md                                    # This file
├── validate_migration_acceptance.py             # Main validation script
├── cdl_migration_acceptance_queries.json        # Business validation queries
├── requirements.txt                             # Python dependencies
├── config.yaml                                  # Configuration (optional)
├── validators/                                  # Validation engine modules
│   ├── table_comparison_validator.py           # Core comparison logic
│   ├── bq_data_loader.py                       # BigQuery data loader
│   └── local_source_validator.py               # Legacy transformation validator
└── scripts/                                     # Utility scripts
```

---

## Additional Tools

### Table Comparison Validator

Generic tool for comparing any two BigQuery tables:

```bash
python3 validate_table_comparison.py \
  --cdl-table "project.dataset.table1" \
  --target-table "project.dataset.table2" \
  --queries-file example_validation_queries.json
```

See `example_validation_queries.json` for query examples.

### Legacy Transformation Validator

Validates CDL by rebuilding transformations from source:

```bash
python3 validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-derived
```

**Note:** Currently disabled due to incorrect mapping document.

---

## Support

For issues or questions:
1. Check the error message in console output
2. Review `cdl_migration_acceptance_report.json` for details
3. Run failing queries manually in BigQuery to investigate
4. Contact the data engineering team

---

**Last Updated:** 2026-03-24
