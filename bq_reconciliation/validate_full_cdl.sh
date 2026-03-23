#!/bin/bash
# Validate Full CDL from BigQuery
# This validates the full CDL table in SIT against source tables in PRD

python3 validate_from_source.py \
  --cdl-source bigquery \
  --cdl-table ncau-data-newsquery-sit.cdl.subscription_transaction_fct \
  --use-bigquery \
  --bigquery-project ncau-data-newsquery-prd \
  --start-date 2026-02-01 \
  --end-date 2026-03-16 \
  --validate-all \
  --llm-report \
  --output validation_full_cdl

echo ""
echo "Validation complete! Results saved to:"
echo "  - validation_full_cdl.html (summary report)"
echo "  - validation_full_cdl.csv (detailed results)"
echo "  - validation_full_cdl_llm.md (comprehensive LLM report)"
echo "  - validation_full_cdl_llm.json (raw data)"
