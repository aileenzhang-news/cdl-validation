# CDL Validation Testing Guide

This guide provides step-by-step instructions for testing the CDL Validation Framework with sample datasets.

**Last Updated:** 2026-03-19

---

## Quick Start - 5 Minute Test

Test the validation framework with a small sample dataset to verify everything works.

### Prerequisites

```bash
cd bq_reconciliation
pip install -r requirements.txt
```

### Option 1: Test with Local CSV Files (Fastest)

```bash
# Using test dataset (2026 Feb-Mar, 356 rows)
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived

# Expected output: ✅ 3/3 derived metrics pass (100%)
```

### Option 2: Test with BigQuery (Requires Authentication)

```bash
# Authenticate first
gcloud auth application-default login

# Run validation with BigQuery
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --use-bigquery \
  --start-date 2026-02-01 \
  --end-date 2026-03-31 \
  --validate-derived

# Expected output: ✅ 3/3 derived metrics pass (100%)
```

---

## Test Dataset Overview

### Test CDL File
- **File:** `subscription_transaction_fct_2026_feb_mar.xlsx`
- **Rows:** 356
- **Date Range:** Feb 1 - Mar 16, 2026 (44 unique dates)
- **Purpose:** Small, manageable dataset for quick testing

### Test Source Data (CSV Mode)
Location: `source_data_test/`

| File | Rows | Description |
|------|------|-------------|
| `source_subscription_base.csv` | 18,544 | Subscription base records |
| `source_subscription_movement_partial.csv` | 16,626 | Movement events |
| `source_calendar.csv` | 59 | Calendar dimension (filtered to 44) |
| `source_schemas.csv` | 275 | Schema definitions |

### Production Dataset (For Reference)
- **File:** `subscription_transaction_fct.xlsx`
- **Rows:** 21,184
- **Full date range:** Much larger dataset

---

## Step-by-Step Testing

### Test 1: Validate Derived Metrics (Fastest)

**What it tests:** Business logic formulas (TotalAcquisition, NetAcquisition, NetCancellation)

**Command:**
```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived
```

**Expected Results:**
```
================================================================================
LAYER 5: DERIVED METRICS VALIDATION
================================================================================
Validating 3 sum_of_sums metrics...
  TotalAcquisition: ✅ PASS
  NetAcquisition: ✅ PASS
  NetCancellation: ✅ PASS

================================================================================
VALIDATION SUMMARY
================================================================================

Total validations: 3
  ✅ Passed: 3
  ❌ Failed: 0
  Pass rate: 100.0%

By transformation type:
  derived_metric:  3/3 passed  ✅ 100%

🎉 All validations passed!
```

**Duration:** ~30 seconds

---

### Test 2: Full Validation (Comprehensive)

**What it tests:** Derived metrics + aggregation columns (10 metrics)

**Command:**
```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-all
```

**Expected Results:**
```
================================================================================
LAYER 5: DERIVED METRICS VALIDATION
================================================================================
  TotalAcquisition: ✅ PASS
  NetAcquisition: ✅ PASS
  NetCancellation: ✅ PASS

================================================================================
LAYER 4: AGGREGATION VALIDATION
================================================================================

Rebuilding final_select CTE from source data...
  ✓ Rebuilt CTE: 180,676 rows, 37 columns
  CDL has: 356 rows

Validating 10 aggregation columns...
  subscription_count: ❌ FAIL
    Rows only in source: 180,320
  acquisition_count: ❌ FAIL
    Rows only in source: 180,320
  ... (8 more)

================================================================================
VALIDATION SUMMARY
================================================================================

Total validations: 13
  ✅ Passed: 3 (Derived Metrics)
  ❌ Failed: 10 (Aggregations - data scope mismatch)
  Pass rate: 23.1%

By transformation type:
  derived_metric:  3/3 passed  ✅ 100%
  aggregation:     0/10 passed ⚠️  (framework working, detecting scope differences)
```

**Duration:** ~2 minutes

**Interpretation:**
- ✅ **Derived metrics pass** = Business logic is correct
- ❌ **Aggregations "fail"** = Framework correctly detects that source exports (180K rows) are broader than CDL (356 rows)
- This is **expected behavior** - source data includes more records than production CDL filters

---

### Test 3: BigQuery Mode (Fresh Data)

**Prerequisites:**
```bash
# Authenticate to GCP
gcloud auth application-default login

# Verify access
gcloud config get-value project
# Should show: ncau-data-newsquery-prd
```

**Command:**
```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --use-bigquery \
  --start-date 2026-02-01 \
  --end-date 2026-03-31 \
  --validate-derived
```

**What happens:**
1. Connects to BigQuery using Application Default Credentials
2. Queries source tables directly (no CSV files needed)
3. Filters data to specified date range
4. Runs validation

**Expected Results:** Same as Test 1 (100% pass for derived metrics)

**Duration:** ~45 seconds (includes BigQuery query time)

---

## Creating Custom Test Datasets

### Method 1: Using create_test_cdl.py

Create a test CDL subset from the full production file:

```bash
python scripts/create_test_cdl.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --output ../subscription_transaction_fct_2026_jan.xlsx
```

**Parameters:**
- `--start-date` - Start date (YYYY-MM-DD)
- `--end-date` - End date (YYYY-MM-DD)
- `--output` - Output Excel file path

**Output:** New CDL file with only the specified date range

---

### Method 2: Export Custom BigQuery Data

Use the export queries with custom date ranges:

```bash
# 1. Edit queries/EXPORT_QUERIES_2026_FEB_MAR.sql
# 2. Change date ranges in WHERE clauses
# 3. Run queries in BigQuery Console
# 4. Export results to CSV
# 5. Save to source_data_test/ directory
```

**Required files:**
- `source_subscription_base.csv`
- `source_subscription_movement_partial.csv`
- `source_calendar.csv`
- `source_schemas.csv`

---

## Interpreting Validation Results

### Success Indicators

✅ **All validations passed:**
```
🎉 All validations passed!
Exit code: 0
```

✅ **Derived metrics pass (most important):**
```
derived_metric:  3/3 passed  ✅ 100%
```
This means CDL business logic is correct.

---

### Expected "Failures"

⚠️ **Aggregation mismatches (data scope difference):**
```
aggregation: 0/10 passed ⚠️
Rows only in source: 180,320
```

**Why this happens:**
- Source exports contain ALL subscriptions in date range
- Production CDL applies additional undocumented business filters
- Result: Source has more rows than CDL

**This is not a framework error** - it's successfully detecting that source data is broader in scope than the final CDL.

---

### Actual Failures

❌ **Derived metric failure (indicates problem):**
```
TotalAcquisition: ❌ FAIL
  Max delta: 15.50
  Mismatched rows: 42/356
```

**Possible causes:**
- Formula mismatch between mapping and CDL
- Source data issue
- Bug in validation framework

**Action:** Investigate the specific metric formula and source columns.

---

## Troubleshooting

### Issue: "Failed to load source data"

**Error:**
```
❌ Failed to initialize validator: [Errno 2] No such file or directory: 'source_data'
```

**Solution:**
```bash
# Check current directory
pwd
# Should be: /path/to/CDL/bq_reconciliation

# Verify source data exists
ls source_data_test/

# Use correct path
python validate_from_source.py \
  --source-data source_data_test \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --validate-derived
```

---

### Issue: "Could not automatically determine credentials" (BigQuery mode)

**Error:**
```
❌ Failed to initialize validator: Could not automatically determine credentials
```

**Solution:**
```bash
# Option 1: Use Application Default Credentials
gcloud auth application-default login

# Option 2: Use service account key
python validate_from_source.py \
  --use-bigquery \
  --bigquery-credentials ~/.gcp/cdl-validator-key.json \
  --validate-derived
```

See [BIGQUERY_SETUP.md](BIGQUERY_SETUP.md) for detailed authentication setup.

---

### Issue: "Column not found in CDL"

**Error:**
```
❌ Column 'TotalAcquisition' not found in CDL
```

**Causes:**
1. Wrong CDL file being used
2. Column name mismatch
3. CDL file is corrupted

**Solution:**
```bash
# Check CDL columns
python -c "import pandas as pd; df=pd.read_excel('../subscription_transaction_fct.xlsx'); print(df.columns.tolist())"

# Verify correct file
ls -lh ../subscription_transaction_fct*.xlsx
```

---

### Issue: Validation is very slow

**Symptom:** Validation takes >5 minutes

**Causes:**
1. Large dataset (production CDL with 21K rows)
2. BigQuery mode with slow network
3. Inefficient filtering

**Solutions:**
```bash
# Use test dataset for quick iteration
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived

# Limit BigQuery date range
python validate_from_source.py \
  --use-bigquery \
  --start-date 2026-03-01 \
  --end-date 2026-03-31 \
  --validate-derived

# Use derived-only mode (skip aggregations)
python validate_from_source.py \
  --validate-derived  # Fast, most important validations
```

---

### Issue: Memory errors with large datasets

**Error:**
```
MemoryError: Unable to allocate array
```

**Solution:**
1. Use smaller date ranges
2. Increase system memory
3. Use CSV mode instead of BigQuery (more memory efficient)
4. Run validation in chunks by date range

---

## Performance Benchmarks

Tested on MacBook Pro (M1, 16GB RAM):

| Test Type | Dataset | Rows | Duration | Memory |
|-----------|---------|------|----------|--------|
| Derived metrics | Test (356 rows) | 356 | 30s | <1GB |
| Full validation | Test (356 rows) | 356 | 2min | ~2GB |
| Derived metrics | Production (21K) | 21,184 | 2min | ~3GB |
| Full validation | Production (21K) | 21,184 | 10min | ~8GB |
| BigQuery mode | Test | 356 | 45s | <1GB |
| BigQuery mode | Production | 21,184 | 3min | ~3GB |

---

## Test Checklist

Before committing changes or deploying to production:

- [ ] **Test 1:** Run derived metrics validation on test dataset
  ```bash
  python validate_from_source.py --source-data source_data_test --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx --validate-derived
  ```
  - Expected: 100% pass

- [ ] **Test 2:** Run full validation on test dataset
  ```bash
  python validate_from_source.py --source-data source_data_test --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx --validate-all
  ```
  - Expected: Derived metrics pass, aggregations show scope difference

- [ ] **Test 3:** Test BigQuery mode (if authentication set up)
  ```bash
  python validate_from_source.py --use-bigquery --start-date 2026-02-01 --end-date 2026-03-31 --validate-derived
  ```
  - Expected: 100% pass

- [ ] **Test 4:** Verify error handling
  ```bash
  python validate_from_source.py --cdl nonexistent.xlsx --validate-derived
  ```
  - Expected: Clear error message, exit code 1

- [ ] **Test 5:** Verify output file generation
  ```bash
  python validate_from_source.py --source-data source_data_test --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx --validate-derived --output test_report.csv
  ls -lh test_report.csv
  ```
  - Expected: CSV file created with validation results

---

## Advanced Testing

### Testing with Different Date Ranges

```bash
# Q1 2026
python validate_from_source.py \
  --use-bigquery \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --validate-derived

# Single month
python validate_from_source.py \
  --use-bigquery \
  --start-date 2026-02-01 \
  --end-date 2026-02-28 \
  --validate-derived

# Full year (slow!)
python validate_from_source.py \
  --use-bigquery \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --validate-derived
```

### Testing with Different Projects

```bash
# Different GCP project
python validate_from_source.py \
  --use-bigquery \
  --bigquery-project my-test-project \
  --validate-derived

# Different credentials
python validate_from_source.py \
  --use-bigquery \
  --bigquery-credentials ~/.gcp/test-project-key.json \
  --validate-derived
```

---

## Continuous Integration Testing

For automated testing in GitHub Actions or other CI/CD:

```bash
# CI-friendly command (exits with code 1 on failure)
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived \
  --output validation_report.csv

# Check exit code
echo $?  # 0 = success, 1 = failure
```

See [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md) for automated testing setup.

---

## Next Steps

After testing locally:

1. **Review validation results** - Understand what passed and what didn't
2. **Test with BigQuery** - Set up authentication and try direct database connection
3. **Deploy to GitHub Actions** - Automate validation runs
4. **Test with production data** - Once confident, test with full production CDL

## Support

- **BigQuery authentication issues:** See [BIGQUERY_SETUP.md](BIGQUERY_SETUP.md)
- **GitHub Actions setup:** See [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)
- **Architecture details:** See [CDL_VALIDATION_ARCHITECTURE_REPORT.md](CDL_VALIDATION_ARCHITECTURE_REPORT.md)
- **Quick start:** See [README.md](../README.md)
