# BigQuery Connection Setup

This guide explains how to connect the CDL Validation Framework directly to BigQuery instead of using local CSV files.

## Benefits of BigQuery Mode

**Advantages:**
- ✅ Always uses fresh data (no manual exports needed)
- ✅ Reduces storage requirements (no large CSV files)
- ✅ Simplifies automation (one less step in the pipeline)
- ✅ Supports date range filtering for targeted validation

**When to use:**
- Automated validation pipelines
- Production environments
- Regular scheduled validations
- Testing with different date ranges

**When to use CSV mode:**
- Development and testing
- Offline validation
- No GCP credentials available
- Fixed dataset validation

---

## Authentication Options

You have two options for authenticating with BigQuery:

### Option 1: Application Default Credentials (ADC) - Recommended

Use your existing GCP authentication. Best for:
- Local development (if you're already logged into GCP)
- Google Cloud environments (Cloud Run, Cloud Functions, GCE)
- GitHub Actions with Workload Identity Federation

**Setup:**

1. **Install Google Cloud SDK** (if not already installed):
   ```bash
   # macOS
   brew install google-cloud-sdk

   # Linux
   curl https://sdk.cloud.google.com | bash

   # Windows
   # Download from https://cloud.google.com/sdk/docs/install
   ```

2. **Login to GCP:**
   ```bash
   gcloud auth application-default login
   ```

3. **Set project:**
   ```bash
   gcloud config set project ncau-data-newsquery-prd
   ```

4. **Run validation:**
   ```bash
   python validate_from_source.py \
     --cdl ../subscription_transaction_fct.xlsx \
     --use-bigquery \
     --validate-derived
   ```

### Option 2: Service Account JSON Key

Use a service account key file. Best for:
- CI/CD pipelines
- Shared environments
- Systems without gcloud CLI

**Setup:**

1. **Create a service account** (GCP Admin):
   - Go to [GCP Console → IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)
   - Select project: `ncau-data-newsquery-prd`
   - Click **Create Service Account**
   - Name: `cdl-validator` (or any name)
   - Click **Create and Continue**

2. **Grant permissions:**
   - Role: **BigQuery Job User** (to run queries)
   - Role: **BigQuery Data Viewer** (to read tables)
   - Click **Continue** → **Done**

3. **Create and download key:**
   - Click on the service account you just created
   - Go to **Keys** tab
   - Click **Add Key** → **Create new key**
   - Choose **JSON** format
   - Click **Create** (key will download)

4. **Secure the key file:**
   ```bash
   # Move to a secure location
   mv ~/Downloads/ncau-data-newsquery-prd-*.json ~/.gcp/cdl-validator-key.json

   # Set restrictive permissions
   chmod 600 ~/.gcp/cdl-validator-key.json
   ```

5. **Run validation:**
   ```bash
   python validate_from_source.py \
     --cdl ../subscription_transaction_fct.xlsx \
     --use-bigquery \
     --bigquery-credentials ~/.gcp/cdl-validator-key.json \
     --validate-derived
   ```

---

## Usage Examples

### Basic Validation (Derived Metrics)

```bash
# Using ADC
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-derived

# Using service account
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --bigquery-credentials ~/.gcp/cdl-validator-key.json \
  --validate-derived
```

### Full Validation (All Layers)

```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-all
```

### Custom Date Range

```bash
# Validate only 2026 Feb-Mar data
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --use-bigquery \
  --start-date 2026-02-01 \
  --end-date 2026-03-31 \
  --validate-all
```

### Custom Project

```bash
# Connect to a different GCP project
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --bigquery-project my-other-project \
  --validate-derived
```

### Save Report

```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-all \
  --output validation_report.csv
```

---

## CLI Options Reference

### BigQuery-Specific Options

| Option | Default | Description |
|--------|---------|-------------|
| `--use-bigquery` | False | Enable BigQuery mode (instead of CSV files) |
| `--bigquery-project` | `ncau-data-newsquery-prd` | GCP project ID |
| `--bigquery-credentials` | None | Path to service account JSON key (optional) |
| `--start-date` | `2000-03-30` | Start date for data filtering (YYYY-MM-DD) |
| `--end-date` | `2026-03-16` | End date for data filtering (YYYY-MM-DD) |

### General Options

| Option | Description |
|--------|-------------|
| `--cdl` | Path to CDL Excel file |
| `--mapping` | Path to mapping.xlsx (default: `../mapping.xlsx`) |
| `--validate-derived` | Quick check (derived metrics only) |
| `--validate-all` | Full validation (derived + aggregations) |
| `--tolerance` | Numeric tolerance for comparisons (default: 0.01) |
| `--output` | Save validation report to CSV file |

---

## GitHub Actions Integration

### Option 1: Workload Identity Federation (Recommended)

Most secure - no service account keys stored in GitHub.

```yaml
- name: Authenticate to Google Cloud
  uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: 'projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME/providers/PROVIDER_NAME'
    service_account: 'cdl-validator@ncau-data-newsquery-prd.iam.gserviceaccount.com'

- name: Run CDL validation
  run: |
    python validate_from_source.py \
      --cdl ../subscription_transaction_fct.xlsx \
      --use-bigquery \
      --validate-derived
```

### Option 2: Service Account Key in Secrets

Simpler but requires storing credentials.

```yaml
- name: Authenticate to Google Cloud
  uses: google-github-actions/auth@v2
  with:
    credentials_json: '${{ secrets.GCP_SA_KEY }}'

- name: Run CDL validation
  run: |
    python validate_from_source.py \
      --cdl ../subscription_transaction_fct.xlsx \
      --use-bigquery \
      --validate-derived
```

**Setup:**
1. Create service account and download JSON key (see Option 2 above)
2. Add to GitHub Secrets:
   - Go to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret**
   - Name: `GCP_SA_KEY`
   - Value: Paste entire JSON key content

---

## Troubleshooting

### Error: "Could not automatically determine credentials"

**Problem:** ADC not configured

**Solution:**
```bash
gcloud auth application-default login
```

### Error: "Permission denied"

**Problem:** Service account lacks required permissions

**Solution:** Add these roles to the service account:
- BigQuery Job User
- BigQuery Data Viewer

### Error: "Invalid JSON key file"

**Problem:** Key file path is incorrect or file is corrupted

**Solution:**
- Verify file path: `ls -la ~/.gcp/cdl-validator-key.json`
- Re-download key from GCP Console
- Check file contains valid JSON: `cat ~/.gcp/cdl-validator-key.json | python -m json.tool`

### Error: "Table not found"

**Problem:** Wrong project or table doesn't exist

**Solution:**
- Verify project ID: `--bigquery-project ncau-data-newsquery-prd`
- Check table exists in BigQuery Console
- Verify you have access to the tables

### Slow query performance

**Problem:** Large date range or inefficient query

**Solution:**
- Narrow date range: `--start-date 2026-01-01 --end-date 2026-03-31`
- Check BigQuery quotas and limits
- Monitor costs in GCP Console

---

## Security Best Practices

1. **Never commit credentials to Git:**
   - Service account keys are in `.gitignore`
   - Use GitHub Secrets for CI/CD

2. **Use least privilege:**
   - Grant only BigQuery Job User + Data Viewer roles
   - Don't use Owner or Editor roles

3. **Rotate credentials regularly:**
   - Delete old service account keys
   - Create new keys every 90 days

4. **Protect key files:**
   ```bash
   chmod 600 ~/.gcp/cdl-validator-key.json
   ```

5. **Use ADC in development:**
   - Avoid creating unnecessary service accounts
   - Use your personal credentials with `gcloud auth`

6. **Monitor access:**
   - Review BigQuery audit logs
   - Set up alerts for unusual activity

---

## Cost Considerations

BigQuery charges for:
- **Query processing:** ~$5 per TB scanned
- **Storage:** ~$0.02 per GB per month (data at rest)

**Typical validation costs:**
- Full validation (all tables, full date range): ~$0.01-0.10 per run
- Derived metrics only: Free (no BigQuery queries)
- Monthly cost (daily validations): ~$1-3

**Cost optimization tips:**
1. Use `--validate-derived` for frequent checks (no BigQuery needed)
2. Limit date range with `--start-date` and `--end-date`
3. Run full validation weekly instead of daily
4. Monitor costs in GCP Console

---

## Comparison: BigQuery vs CSV

| Feature | BigQuery Mode | CSV Mode |
|---------|---------------|----------|
| **Setup complexity** | Medium (need GCP auth) | Low (just download CSVs) |
| **Data freshness** | Always current | Requires manual export |
| **Storage** | None (query on demand) | Large CSV files (~GB) |
| **Speed** | Network dependent | Fast (local files) |
| **Cost** | ~$0.01-0.10 per run | Free (after export) |
| **Offline support** | No | Yes |
| **Date filtering** | Flexible (`--start-date`) | Fixed (pre-exported) |
| **Automation** | Easier (one step) | Requires export step |

---

## Next Steps

1. ✅ Choose authentication method (ADC or service account)
2. ⬜ Set up authentication (follow steps above)
3. ⬜ Test connection with a quick validation:
   ```bash
   python validate_from_source.py --use-bigquery --validate-derived
   ```
4. ⬜ Update automation scripts/workflows
5. ⬜ Document your chosen auth method for team

## Support

For BigQuery connection issues:
- Check [GCP BigQuery documentation](https://cloud.google.com/bigquery/docs)
- Review [Authentication documentation](https://cloud.google.com/docs/authentication)
- See main [README.md](../README.md) for framework usage
