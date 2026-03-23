# CDL Validation Automation Setup

This guide explains how to set up and configure the automated CDL validation pipeline using GitHub Actions.

## Overview

The workflow ([.github/workflows/cdl-validation.yml](.github/workflows/cdl-validation.yml)) automates the CDL validation process to ensure data quality and catch issues early.

## Quick Start

### 1. Manual Trigger

Run the validation manually from GitHub:

1. Go to **Actions** tab in your GitHub repository
2. Select **CDL Validation Pipeline**
3. Click **Run workflow**
4. Choose validation mode:
   - **derived**: Quick check (derived metrics only) - ~30 seconds
   - **full**: Complete validation (derived + aggregations) - ~2 minutes

### 2. View Results

After the workflow runs:
- **Job Summary**: View results directly in the Actions run page
- **Validation Report**: Download the CSV artifact from the workflow run
- **Exit Code**: 0 = passed, 1 = failed

## Configuration Options

### Trigger Options

**Current setup**: Manual trigger only

**Available options** (uncomment in workflow file):

```yaml
# Weekly schedule (every Monday at 9 AM UTC)
schedule:
  - cron: '0 9 * * 1'

# On CDL file changes
push:
  paths:
    - 'subscription_transaction_fct.xlsx'
    - 'bq_reconciliation/source_data/**'
```

### Data Storage Options

#### Option 1: Store in Repository (Current)
- ✅ Simple setup
- ✅ Version controlled
- ⚠️  Limited to files < 100 MB
- **Files needed**:
  - `subscription_transaction_fct.xlsx`
  - `bq_reconciliation/source_data/*.csv`

#### Option 2: Google Cloud Storage
Uncomment the "Download source data" step in the workflow:

```yaml
- name: Download source data
  run: |
    gsutil -m cp -r gs://your-bucket/source_data bq_reconciliation/
    gsutil cp gs://your-bucket/subscription_transaction_fct.xlsx .
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY }}
```

**Setup**:
1. Create a GCP service account with Storage Object Viewer role
2. Download the JSON key
3. Add to GitHub Secrets as `GCP_SA_KEY`:
   - Go to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret**
   - Name: `GCP_SA_KEY`
   - Value: Paste entire JSON key content

#### Option 3: GitHub Artifacts
Store data as workflow artifacts and download in validation runs.

### Notification Options

#### Slack Notifications

Uncomment the Slack notification step:

```yaml
- name: Notify on failure
  if: failure()
  uses: slackapi/slack-github-action@v1
  with:
    webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
```

**Setup**:
1. Create a Slack incoming webhook: https://api.slack.com/messaging/webhooks
2. Add to GitHub Secrets as `SLACK_WEBHOOK_URL`

#### GitHub Issues

Uncomment the "Create issue on failure" step to automatically create an issue when validation fails.

#### Email Notifications

GitHub sends email notifications by default for workflow failures if you're watching the repository.

## Workflow Details

### Steps

1. **Checkout repository** - Get latest code
2. **Set up Python** - Install Python 3.11 with pip caching
3. **Install dependencies** - Install requirements from `requirements.txt`
4. **Download data** (optional) - Fetch source data from external storage
5. **Run validation** - Execute validation script
6. **Upload report** - Save validation report as artifact (90-day retention)
7. **Generate summary** - Create readable summary in Actions UI
8. **Send notifications** (optional) - Alert on failures
9. **Check status** - Return exit code based on results

### Validation Modes

| Mode | Layers | Duration | Use Case |
|------|--------|----------|----------|
| `derived` | Layer 5 only | ~30s | Quick daily checks |
| `full` | Layers 4 + 5 | ~2min | Weekly comprehensive validation |

### Artifacts

Validation reports are stored as artifacts:
- **Name**: `validation-report-{run_number}`
- **Retention**: 90 days
- **Download**: From the workflow run page

## Customization

### Change Python Version

```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: '3.12'  # Change version here
```

### Change Validation Flags

Edit the `run` section:

```yaml
run: |
  python validate_from_source.py \
    --cdl ../subscription_transaction_fct.xlsx \
    --source-data source_data \
    --validate-derived \
    --output validation_report.csv
```

### Add More Validation Modes

Add to `workflow_dispatch.inputs`:

```yaml
inputs:
  validation_mode:
    type: choice
    options:
      - derived
      - full
      - custom  # Add new mode
```

Then add a corresponding step.

## Troubleshooting

### Workflow fails with "File not found"

**Issue**: CDL or source data files missing

**Solutions**:
- Ensure files are committed to the repository, OR
- Configure external data download step, OR
- Check file paths match the workflow configuration

### Validation takes too long

**Issue**: Full validation times out

**Solutions**:
- Use `derived` mode for frequent checks
- Increase timeout: Add `timeout-minutes: 30` to the job
- Optimize source data size (use test dataset)

### No notifications received

**Issue**: Slack/email notifications not working

**Solutions**:
- Verify secrets are correctly configured
- Check webhook URL is valid
- Ensure notification steps are uncommented

## Best Practices

1. **Start with manual triggers** - Test thoroughly before enabling schedules
2. **Use derived mode for frequent checks** - Fast feedback loop
3. **Run full validation weekly** - Comprehensive coverage
4. **Monitor artifact storage** - Clean up old reports periodically
5. **Version control data** - Commit test datasets for reproducibility

## Security

- **Never commit credentials** - Always use GitHub Secrets
- **Use service accounts** - Limit permissions to read-only
- **Rotate secrets regularly** - Update GCP keys and webhooks
- **Review workflow logs** - Ensure no sensitive data is logged

## Next Steps

1. ✅ Review and customize the workflow file
2. ⬜ Set up data storage (repo or GCS)
3. ⬜ Configure notifications (Slack/issues)
4. ⬜ Test with manual trigger
5. ⬜ Enable scheduled runs (optional)
6. ⬜ Document any custom changes

## Support

For workflow issues:
- Check the Actions tab for detailed logs
- Review [GitHub Actions documentation](https://docs.github.com/en/actions)
- See [CDL Validation docs](../bq_reconciliation/docs/)
