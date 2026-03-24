#!/usr/bin/env python3
"""
Validate CDL Table vs Target Table Comparison

Validates data consistency between CDL table and target table in BigQuery by:
1. Schema check - column names, types, nullability
2. Quantity check - row counts, distinct values, null rates
3. Query-result matching - run same queries on both tables and compare
"""

import argparse
import sys
from pathlib import Path

from validators.table_comparison_validator import TableComparisonValidator


def main():
    parser = argparse.ArgumentParser(
        description='Validate CDL table against target table in BigQuery'
    )

    # Table paths
    parser.add_argument(
        '--cdl-table',
        default='ncau-data-newsquery-sit.cdl.subscription_transaction_fct',
        help='Full path to CDL table (project.dataset.table)'
    )

    parser.add_argument(
        '--target-table',
        default='ncau-data-newsquery-prd.prstn_consumer.subscription_base_movement_agg_snap',
        help='Full path to target table (project.dataset.table)'
    )

    # BigQuery options
    parser.add_argument(
        '--project',
        default='ncau-data-newsquery-sit',
        help='GCP project ID (default: ncau-data-newsquery-sit)'
    )

    parser.add_argument(
        '--credentials',
        help='Path to GCP service account JSON key file (optional, uses Application Default Credentials if not provided)'
    )

    # Validation options
    parser.add_argument(
        '--validate-schema',
        action='store_true',
        help='Only validate schema'
    )

    parser.add_argument(
        '--validate-quantities',
        action='store_true',
        help='Only validate quantities'
    )

    parser.add_argument(
        '--validate-queries',
        action='store_true',
        help='Only validate query results'
    )

    parser.add_argument(
        '--tolerance',
        type=float,
        default=0.01,
        help='Numeric tolerance for comparisons (default: 0.01)'
    )

    # Custom queries file
    parser.add_argument(
        '--queries-file',
        help='Path to JSON file with custom validation queries'
    )

    # Output
    parser.add_argument(
        '--output',
        default='table_comparison_report',
        help='Output report file prefix (default: table_comparison_report)'
    )

    args = parser.parse_args()

    # Default to all validations if nothing specified
    validate_all = not (args.validate_schema or args.validate_quantities or args.validate_queries)

    print("=" * 80)
    print("CDL TABLE COMPARISON VALIDATION")
    print("=" * 80)
    print(f"\nCDL table: {args.cdl_table}")
    print(f"Target table: {args.target_table}")
    print(f"Project: {args.project}")
    if args.credentials:
        print(f"Credentials: {args.credentials}")
    else:
        print(f"Credentials: Application Default Credentials")
    print(f"Tolerance: {args.tolerance}")

    # Initialize validator
    try:
        validator = TableComparisonValidator(
            project_id=args.project,
            cdl_table=args.cdl_table,
            target_table=args.target_table,
            credentials_path=args.credentials,
            tolerance=args.tolerance
        )
    except Exception as e:
        print(f"\n❌ Failed to initialize validator: {e}")
        import traceback
        traceback.print_exc()
        return 1

    all_results = []

    # Run validations
    if args.validate_schema or validate_all:
        try:
            schema_results = validator.validate_schema()
            all_results.extend(schema_results)
        except Exception as e:
            print(f"\n❌ Schema validation error: {e}")
            import traceback
            traceback.print_exc()

    if args.validate_quantities or validate_all:
        try:
            quantity_results = validator.validate_quantities()
            all_results.extend(quantity_results)
        except Exception as e:
            print(f"\n❌ Quantity validation error: {e}")
            import traceback
            traceback.print_exc()

    if args.validate_queries or validate_all:
        try:
            # Load custom queries if provided
            custom_queries = None
            if args.queries_file:
                import json
                with open(args.queries_file, 'r') as f:
                    custom_queries = json.load(f)
                print(f"\nLoaded {len(custom_queries)} custom queries from {args.queries_file}")

            query_results = validator.validate_query_results(questions=custom_queries)
            all_results.extend(query_results)
        except Exception as e:
            print(f"\n❌ Query validation error: {e}")
            import traceback
            traceback.print_exc()

    # Generate summary report
    if all_results:
        summary = validator.generate_report(all_results, args.output)

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
