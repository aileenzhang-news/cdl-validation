#!/usr/bin/env python3
"""
Validate CDL from Source Tables

Validates subscription_transaction_fct.xlsx against source tables
using either:
- Local CSV exports (default)
- Direct BigQuery connection (with --use-bigquery flag)
"""

import argparse
import sys
from pathlib import Path
import pandas as pd

from validators.local_source_validator import LocalSourceValidator


def main():
    parser = argparse.ArgumentParser(
        description='Validate CDL against source tables (local CSV files or BigQuery)'
    )

    # Input files
    parser.add_argument(
        '--cdl',
        default='../subscription_transaction_fct.xlsx',
        help='Path to CDL Excel file (used when --cdl-source=excel)'
    )

    parser.add_argument(
        '--cdl-source',
        choices=['excel', 'bigquery'],
        default='excel',
        help='Source of CDL data: excel (default) or bigquery'
    )

    parser.add_argument(
        '--cdl-table',
        help='BigQuery table path for CDL (e.g., project.dataset.table, required when --cdl-source=bigquery)'
    )

    parser.add_argument(
        '--mapping',
        default='../mapping.xlsx',
        help='Path to mapping.xlsx'
    )

    # Data source options
    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument(
        '--source-data',
        help='Directory containing source CSV files (default mode)'
    )

    data_source.add_argument(
        '--use-bigquery',
        action='store_true',
        help='Load data directly from BigQuery instead of local CSVs'
    )

    # BigQuery options (only used with --use-bigquery)
    parser.add_argument(
        '--bigquery-project',
        default='ncau-data-newsquery-prd',
        help='GCP project ID for BigQuery (default: ncau-data-newsquery-prd)'
    )

    parser.add_argument(
        '--bigquery-credentials',
        help='Path to GCP service account JSON key file (optional, uses Application Default Credentials if not provided)'
    )

    parser.add_argument(
        '--start-date',
        default='2026-02-01',
        help='Start date for BigQuery data filtering (YYYY-MM-DD, default: 2026-02-01)'
    )

    parser.add_argument(
        '--end-date',
        default='2026-03-16',
        help='End date for BigQuery data filtering (YYYY-MM-DD, default: 2026-03-16)'
    )

    # Validation options
    parser.add_argument(
        '--validate-schema',
        action='store_true',
        help='Only validate schema'
    )

    parser.add_argument(
        '--validate-derived',
        action='store_true',
        help='Only validate derived metrics'
    )

    parser.add_argument(
        '--validate-all',
        action='store_true',
        help='Run all validations'
    )

    parser.add_argument(
        '--tolerance',
        type=float,
        default=0.01,
        help='Numeric tolerance for comparisons (default: 0.01)'
    )

    parser.add_argument(
        '--output',
        help='Output report file prefix (will create .html and .csv)'
    )

    # LLM Report options
    parser.add_argument(
        '--llm-report',
        action='store_true',
        help='Generate comprehensive testing report using local LLM (requires Ollama)'
    )

    parser.add_argument(
        '--llm-model',
        default='llama3.2:3b',
        help='Ollama model to use for report generation (default: llama3.2:3b)'
    )

    parser.add_argument(
        '--previous-report',
        help='Path to previous validation results JSON for comparison'
    )

    args = parser.parse_args()

    # Set default source-data if neither option is specified
    if not args.use_bigquery and not args.source_data:
        args.source_data = 'source_data'

    # Default to validate-all if nothing specified
    if not (args.validate_schema or args.validate_derived or args.validate_all):
        args.validate_all = True

    print("=" * 80)
    print("CDL SOURCE VALIDATION")
    print("=" * 80)

    # Validate CDL source arguments
    if args.cdl_source == 'bigquery' and not args.cdl_table:
        print("❌ Error: --cdl-table is required when --cdl-source=bigquery")
        return 1

    print(f"\nCDL source: {args.cdl_source.upper()}")
    if args.cdl_source == 'excel':
        print(f"  CDL file: {args.cdl}")
    else:
        print(f"  CDL table: {args.cdl_table}")

    print(f"Mapping: {args.mapping}")

    if args.use_bigquery:
        print(f"\nSource data: BigQuery")
        print(f"  Project: {args.bigquery_project}")
        print(f"  Date range: {args.start_date} to {args.end_date}")
        if args.bigquery_credentials:
            print(f"  Credentials: {args.bigquery_credentials}")
        else:
            print(f"  Credentials: Application Default Credentials")
    else:
        print(f"\nSource data: Local CSV files")
        print(f"  Directory: {args.source_data}")

    print(f"\nTolerance: {args.tolerance}")

    # Initialize validator
    try:
        validator = LocalSourceValidator(
            mapping_file=args.mapping,
            source_data_dir=args.source_data,
            tolerance=args.tolerance,
            use_bigquery=args.use_bigquery,
            bigquery_project=args.bigquery_project,
            bigquery_credentials=args.bigquery_credentials,
            start_date=args.start_date,
            end_date=args.end_date
        )
    except Exception as e:
        print(f"\n❌ Failed to initialize validator: {e}")
        import traceback
        traceback.print_exc()
        return 1

    all_results = []

    # Layer 1: Schema validation
    if args.validate_schema or args.validate_all:
        try:
            schema_result = validator.validate_schema()
            if schema_result['status'] == 'failed':
                print("\n⚠️  Schema validation failed - some columns missing from source tables")
                print("   Continuing with value validation anyway...")
        except Exception as e:
            print(f"\n❌ Schema validation error: {e}")
            import traceback
            traceback.print_exc()

    # Load CDL data
    print(f"\n{'='*80}")
    print("LOADING CDL DATA")
    print("=" * 80)

    try:
        if args.cdl_source == 'excel':
            print(f"Loading from Excel: {args.cdl}")
            cdl_df = pd.read_excel(args.cdl)
            cdl_df['report_date'] = pd.to_datetime(cdl_df['report_date'])
        else:
            # Load from BigQuery
            print(f"Querying BigQuery: {args.cdl_table}")
            from google.cloud import bigquery

            # Initialize BigQuery client
            if args.bigquery_credentials:
                bq_client = bigquery.Client.from_service_account_json(args.bigquery_credentials)
            else:
                bq_client = bigquery.Client(project=args.bigquery_project)

            # Query the full CDL table
            query = f"""
            SELECT *
            FROM `{args.cdl_table}`
            WHERE report_date BETWEEN '{args.start_date}' AND '{args.end_date}'
            ORDER BY report_date
            """

            print(f"  Date filter: {args.start_date} to {args.end_date}")
            cdl_df = bq_client.query(query).to_dataframe()
            cdl_df['report_date'] = pd.to_datetime(cdl_df['report_date'])

        print(f"✓ Loaded CDL: {len(cdl_df):,} rows, {len(cdl_df.columns)} columns")
        print(f"  Date range: {cdl_df['report_date'].min().date()} to {cdl_df['report_date'].max().date()}")

    except Exception as e:
        print(f"❌ Failed to load CDL: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Layer 5: Derived metrics (always fast, no BigQuery needed)
    if args.validate_derived or args.validate_all:
        try:
            derived_results = validator.validate_derived_metrics(cdl_df)
            all_results.extend(derived_results)
        except Exception as e:
            print(f"\n❌ Derived metrics validation error: {e}")
            import traceback
            traceback.print_exc()

    # Layer 2-3: Simple column validation
    if args.validate_all:
        try:
            if hasattr(validator, 'validate_simple_columns'):
                simple_results = validator.validate_simple_columns(cdl_df)
                all_results.extend(simple_results)
        except Exception as e:
            print(f"\n❌ Simple column validation error: {e}")
            import traceback
            traceback.print_exc()

    # Layer 4: Aggregation column validation
    if args.validate_all:
        try:
            if hasattr(validator, 'validate_aggregation_columns'):
                agg_results = validator.validate_aggregation_columns(cdl_df)
                all_results.extend(agg_results)
        except Exception as e:
            print(f"\n❌ Aggregation column validation error: {e}")
            import traceback
            traceback.print_exc()

    # Generate summary report
    if all_results:
        summary = validator.generate_report(all_results, args.output)

        # Generate LLM-powered comprehensive report
        if args.llm_report:
            try:
                from validators.llm_report_generator import LLMReportGenerator

                print(f"\n{'='*80}")
                print("GENERATING LLM-POWERED TESTING REPORT")
                print("=" * 80)

                # Initialize LLM report generator
                llm_gen = LLMReportGenerator(model=args.llm_model)

                # Check Ollama availability
                if not llm_gen.check_ollama_status():
                    print("\n⚠️  Ollama is not running!")
                    print("To set up Ollama:")
                    print("  1. Run: bash setup_ollama.sh")
                    print("  2. Or install manually from: https://ollama.com")
                    print("\nSkipping LLM report generation...")
                else:
                    available_models = llm_gen.list_available_models()
                    print(f"✓ Ollama is running")
                    print(f"✓ Using model: {args.llm_model}")
                    print(f"  Available models: {', '.join(available_models)}")

                    # Add validation results
                    for result in all_results:
                        llm_gen.add_validation_result(
                            metric_name=result.column_name,
                            passed=bool(result.passed),  # Convert numpy bool to Python bool
                            details={
                                'column_name': result.column_name,
                                'cte_name': result.cte_name,
                                'transformation_type': result.transformation_type,
                                'passed': bool(result.passed),  # Convert numpy bool to Python bool
                                'total_rows': int(result.total_rows) if result.total_rows else 0,
                                'mismatched_rows': int(result.mismatched_rows) if result.mismatched_rows else 0,
                                'max_delta': float(result.max_delta) if result.max_delta else 0.0,
                                'error_message': str(result.error_message) if result.error_message else ''
                            }
                        )

                    # Add data quality metrics
                    llm_gen.add_metrics({
                        'cdl_rows': len(cdl_df),
                        'cdl_date_range': f"{cdl_df['report_date'].min().date()} to {cdl_df['report_date'].max().date()}",
                        'source_data_mode': 'BigQuery' if args.use_bigquery else 'CSV',
                        'validation_timestamp': pd.Timestamp.now().isoformat()
                    })

                    # Load previous results if available
                    if args.previous_report:
                        llm_gen.load_previous_results(args.previous_report)

                    # Generate comprehensive report
                    report = llm_gen.generate_comprehensive_report()

                    # Save report
                    output_prefix = args.output or 'validation_report'
                    report_path = f"{output_prefix}_llm.md"
                    llm_gen.save_report(report_path, report)

                    print(f"\n✅ LLM report generated: {report_path}")
                    print(f"✅ Raw data saved: {report_path.replace('.md', '.json')}")

            except ImportError:
                print("\n⚠️  LLM report generator not available")
            except Exception as e:
                print(f"\n⚠️  Failed to generate LLM report: {e}")
                import traceback
                traceback.print_exc()

        # Return exit code based on results
        if summary['failed'] == 0:
            print("\n🎉 All validations passed!")
            return 0
        else:
            print(f"\n⚠️  {summary['failed']} validation(s) failed")
            return 1
    else:
        print("\n⚠️  No validations were run")
        return 1


if __name__ == '__main__':
    sys.exit(main())
