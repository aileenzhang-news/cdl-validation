# LLM-Powered Testing Report Generation

This guide explains how to set up and use AI-powered comprehensive testing reports for CDL validation.

**Last Updated:** 2026-03-20

---

## Overview

The CDL Validation Framework can generate **comprehensive, AI-analyzed testing reports** using a local LLM (Large Language Model). This provides:

✅ **Executive summaries** - Non-technical overviews for stakeholders
✅ **Detailed findings** - Root cause analysis of failures
✅ **Data quality metrics** - Statistical analysis
✅ **Recommendations** - Specific fix suggestions
✅ **Trend analysis** - Comparison with previous runs
✅ **Risk assessment** - Business impact evaluation

---

## Benefits

### Traditional Report
```
Validation Summary:
- Total: 3
- Passed: 3
- Failed: 0
```

### LLM-Enhanced Report
```
EXECUTIVE SUMMARY

The CDL validation demonstrates excellent data quality with 100% pass rate
across all 3 derived metrics. The subscription data pipeline is operating
within expected parameters. Key findings:

• All critical business metrics (TotalAcquisition, NetAcquisition,
  NetCancellation) validated successfully
• Data coverage spans 356 records from Feb-Mar 2026
• No data quality issues detected

RISK ASSESSMENT: LOW - Continue standard monitoring protocols.

RECOMMENDATIONS

1. Implement automated daily validations to maintain quality
2. Establish baseline metrics for future comparisons
3. Consider expanding validation coverage to aggregation layers
```

---

## Quick Start

### Step 1: Install Ollama (Local LLM)

**macOS:**
```bash
cd bq_reconciliation
bash setup_ollama.sh
```

**Manual Installation:**
1. Download from [https://ollama.com](https://ollama.com)
2. Install Ollama.app
3. Open Ollama to start the service
4. Run: `ollama pull llama3.2:3b`

### Step 2: Verify Ollama is Running

```bash
# Check if Ollama service is running
curl http://localhost:11434/api/tags

# List installed models
ollama list
```

Expected output:
```
NAME              SIZE      MODIFIED
llama3.2:3b       2.0 GB    5 minutes ago
```

### Step 3: Run Validation with LLM Report

```bash
# With BigQuery (using your AD credentials)
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --use-bigquery \
  --start-date 2026-02-01 \
  --end-date 2026-02-28 \
  --validate-derived \
  --llm-report

# With local CSV files
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived \
  --llm-report
```

### Step 4: View the Report

The LLM report will be generated as:
- `validation_report_llm.md` - Markdown report (human-readable)
- `validation_report_llm.json` - Raw data (for comparisons)

```bash
# View the report
cat validation_report_llm.md

# Or open in editor
open validation_report_llm.md
```

---

## Recommended Models

| Model | Size | Speed | Quality | Use Case |
|-------|------|-------|---------|----------|
| **llama3.2:1b** | 1 GB | ⚡⚡⚡ Fast | ⭐⭐ Basic | Quick drafts |
| **llama3.2:3b** | 2 GB | ⚡⚡ Medium | ⭐⭐⭐ Good | **Recommended** |
| **mistral:7b** | 4 GB | ⚡ Slow | ⭐⭐⭐⭐ Better | Detailed analysis |
| **llama3.1:8b** | 5 GB | ⚡ Slow | ⭐⭐⭐⭐⭐ Best | Production reports |

### Switching Models

```bash
# Download a different model
ollama pull mistral:7b

# Use it for report generation
python validate_from_source.py \
  --llm-report \
  --llm-model mistral:7b \
  --validate-derived
```

---

## Advanced Usage

### Compare with Previous Run

Track quality trends over time:

```bash
# First run - save results
python validate_from_source.py \
  --validate-derived \
  --llm-report \
  --output baseline_feb2026

# Second run - compare with baseline
python validate_from_source.py \
  --validate-derived \
  --llm-report \
  --output current_run \
  --previous-report baseline_feb2026_llm.json
```

The LLM will analyze:
- Improvement or degradation in pass rates
- New issues introduced
- Resolved issues
- Persistent problems
- Overall data quality trajectory

### Full Validation with Comprehensive Report

```bash
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-all \
  --llm-report \
  --output full_validation_report
```

This generates a complete analysis including:
- Derived metrics validation
- Aggregation validation
- Cross-layer analysis
- Comprehensive risk assessment

---

## Report Sections Explained

### 1. Executive Summary
**Audience:** Senior management, non-technical stakeholders
**Content:** High-level status (Green/Yellow/Red), key findings, business impact

### 2. Validation Statistics
**Audience:** Data engineers, analysts
**Content:** Pass/fail counts, rates, numerical metrics

### 3. Data Quality Metrics
**Audience:** Technical teams
**Content:** Row counts, data ranges, coverage statistics

### 4. Detailed Findings
**Audience:** Data engineers
**Content:** Root cause analysis, technical explanations, data issues

### 5. Recommendations
**Audience:** All
**Content:** Prioritized action items, short/medium/long-term fixes

### 6. Comparison with Previous Run
**Audience:** Data quality team
**Content:** Trends, trajectory, regression detection

### 7. Risk Assessment
**Audience:** Management, data governance
**Content:** Business impact, data trust implications, urgency levels

### 8. Validation Details
**Audience:** Technical teams
**Content:** Raw validation results, metrics, thresholds

---

## Troubleshooting

### Issue: "Ollama is not running"

**Solution:**
```bash
# macOS - Launch Ollama app
open -a Ollama

# Linux - Start Ollama service
ollama serve &

# Verify it's running
curl http://localhost:11434/api/tags
```

### Issue: "Model not found"

**Solution:**
```bash
# List available models
ollama list

# Pull the required model
ollama pull llama3.2:3b

# Verify
ollama list
```

### Issue: Report generation is slow

**Causes:**
- Large model (7B or 8B parameters)
- First-time model load (needs to load into memory)
- Many failed validations (more analysis required)

**Solutions:**
```bash
# Use a smaller, faster model
ollama pull llama3.2:1b
python validate_from_source.py --llm-report --llm-model llama3.2:1b

# Or be patient - first generation is slow, subsequent ones are faster
```

### Issue: Report quality is poor

**Solution:**
```bash
# Use a larger, more capable model
ollama pull llama3.1:8b
python validate_from_source.py --llm-report --llm-model llama3.1:8b
```

### Issue: Out of memory errors

**Cause:** Model is too large for your system
**Solution:**
```bash
# Use smallest model
ollama pull llama3.2:1b

# Or upgrade system RAM (8GB minimum recommended)
```

---

## Performance Benchmarks

Tested on MacBook Pro (M1, 16GB RAM):

| Model | First Run | Subsequent Runs | RAM Usage | Quality |
|-------|-----------|-----------------|-----------|---------|
| llama3.2:1b | 15s | 8s | 1.5 GB | Basic |
| llama3.2:3b | 30s | 15s | 2.5 GB | Good |
| mistral:7b | 90s | 45s | 5 GB | Better |
| llama3.1:8b | 120s | 60s | 6 GB | Best |

---

## Cost Comparison

| Solution | Cost per Report | Pros | Cons |
|----------|----------------|------|------|
| **Local LLM (Ollama)** | $0 | Free, private, no API limits | Requires local setup, slower |
| **Claude API** | $0.01-0.10 | Fast, highest quality | Requires API key, costs money |
| **OpenAI GPT-4** | $0.05-0.20 | Very good quality | Requires API key, expensive |

**Recommendation:** Start with local LLM (Ollama) for development and testing. Consider cloud APIs for production if speed is critical.

---

## Security & Privacy

✅ **All data stays local** - No data sent to external servers
✅ **No API keys required** - Completely self-contained
✅ **Offline capable** - Works without internet connection
✅ **GDPR compliant** - No data sharing

---

## Integration with GitHub Actions

You can run LLM report generation in CI/CD:

```yaml
- name: Install Ollama
  run: |
    curl -fsSL https://ollama.com/install.sh | sh
    ollama pull llama3.2:3b

- name: Run validation with LLM report
  run: |
    python validate_from_source.py \
      --use-bigquery \
      --validate-derived \
      --llm-report \
      --output validation_report

- name: Upload LLM report
  uses: actions/upload-artifact@v4
  with:
    name: llm-validation-report
    path: validation_report_llm.md
```

**Note:** GitHub Actions runners may have limited resources. Use `llama3.2:1b` for CI/CD.

---

## Future Enhancements

Planned features:
- [ ] HTML report output with visualizations
- [ ] Email report delivery
- [ ] Slack notification integration
- [ ] Custom report templates
- [ ] Multi-model comparison
- [ ] Historical trend visualization

---

## Support

For issues or questions:
- **Ollama setup:** See [Ollama documentation](https://github.com/ollama/ollama)
- **Report generation:** Check [validators/llm_report_generator.py](../validators/llm_report_generator.py)
- **Framework issues:** See main [README.md](../README.md)

---

## Examples

### Example 1: Quick Quality Check

```bash
# 2-minute validation with AI report
python validate_from_source.py \
  --cdl ../subscription_transaction_fct_2026_feb_mar.xlsx \
  --source-data source_data_test \
  --validate-derived \
  --llm-report
```

**Output:** `validation_report_llm.md` with executive summary and recommendations

### Example 2: Weekly Production Report

```bash
# Comprehensive weekly validation
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --start-date $(date -d '7 days ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --validate-all \
  --llm-report \
  --llm-model llama3.1:8b \
  --output weekly_$(date +%Y%m%d) \
  --previous-report last_week_llm.json
```

**Output:** Comprehensive report with trend analysis

### Example 3: Post-Incident Analysis

```bash
# Detailed investigation after data issue
python validate_from_source.py \
  --cdl ../subscription_transaction_fct.xlsx \
  --use-bigquery \
  --validate-all \
  --llm-report \
  --llm-model mistral:7b \
  --output incident_analysis
```

**Output:** Deep analysis with root cause investigation

---

**Ready to generate your first AI-powered testing report?**

```bash
bash setup_ollama.sh
```
