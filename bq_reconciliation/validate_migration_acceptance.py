#!/usr/bin/env python3
"""
CDL Migration Acceptance Validator

Validates that CDL (subscription_transaction_fct) produces equivalent results to
PRSTN (subscription_base_movement_agg_snap) — the layer being replaced.

This is the PRIMARY MIGRATION ACCEPTANCE CHECK.
A pass here means CDL can safely replace PRSTN as the source of truth for subscription data.

Validation Method: Query result matching
- Run business questions on CDL table
- Run same questions on PRSTN table
- Assert results are equal within tolerance
"""

import sys
import json
from datetime import datetime
from pathlib import Path
import pandas as pd

from validators.table_comparison_validator import TableComparisonValidator


def load_config(config_file: str = 'config.json') -> dict:
    """
    Load configuration from JSON file

    Args:
        config_file: Path to configuration file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: Config file '{config_file}' not found. Using command-line arguments or defaults.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid JSON in config file '{config_file}': {e}")
        return {}


class MigrationAcceptanceValidator:
    """
    Validates CDL migration acceptance by comparing business query results
    between CDL and PRSTN tables
    """

    def __init__(
        self,
        project_id: str,
        cdl_table: str,
        prstn_table: str,
        credentials_path: str = None,
        tolerance: float = None,
        queries_file: str = None,
        date_filter_start: str = None,
        date_filter_end: str = None
    ):
        """
        Initialize migration acceptance validator

        Args:
            project_id: GCP project ID
            cdl_table: Full path to CDL table (the new layer)
            prstn_table: Full path to PRSTN table (the layer being replaced)
            credentials_path: Optional path to service account JSON key
            tolerance: Numeric tolerance for comparisons
            queries_file: Path to business validation queries JSON file
            date_filter_start: Optional start date for partition filtering (YYYY-MM-DD)
            date_filter_end: Optional end date for partition filtering (YYYY-MM-DD)
        """
        self.project_id = project_id
        self.cdl_table = cdl_table
        self.prstn_table = prstn_table
        self.tolerance = tolerance
        self.queries_file = queries_file
        self.date_filter_start = date_filter_start
        self.date_filter_end = date_filter_end

        # Initialize the underlying table comparison validator
        self.validator = TableComparisonValidator(
            project_id=project_id,
            cdl_table=cdl_table,
            target_table=prstn_table,
            credentials_path=credentials_path,
            tolerance=tolerance
        )

        print("=" * 80)
        print("CDL MIGRATION ACCEPTANCE VALIDATION")
        print("=" * 80)
        print(f"\n🆕 NEW LAYER (CDL): {cdl_table}")
        print(f"📦 OLD LAYER (PRSTN): {prstn_table}")
        print(f"🎯 Tolerance: {tolerance}")
        print(f"📋 Queries: {queries_file}")
        if date_filter_start or date_filter_end:
            print(f"📅 Date Filter: {date_filter_start or 'N/A'} to {date_filter_end or 'N/A'}")
        print("\n" + "=" * 80)

    def load_business_queries(self):
        """Load business validation queries from JSON file"""
        try:
            with open(self.queries_file, 'r') as f:
                queries = json.load(f)

            # Apply date filter to queries if specified
            if self.date_filter_start or self.date_filter_end:
                queries = self._apply_date_filter_to_queries(queries)

            print(f"\n✓ Loaded {len(queries)} business validation queries")

            # Count critical vs non-critical
            critical = sum(1 for q in queries if q.get('critical', False))
            non_critical = len(queries) - critical
            print(f"  • Critical queries: {critical}")
            print(f"  • Non-critical queries: {non_critical}")

            return queries

        except FileNotFoundError:
            print(f"\n❌ ERROR: Queries file not found: {self.queries_file}")
            print("Using default queries instead...")
            return None
        except json.JSONDecodeError as e:
            print(f"\n❌ ERROR: Invalid JSON in queries file: {e}")
            print("Using default queries instead...")
            return None

    def _apply_date_filter_to_queries(self, queries):
        """
        Apply date filter to queries by injecting WHERE clause

        Args:
            queries: List of query dictionaries

        Returns:
            Modified queries with date filter
        """
        filtered_queries = []

        for query in queries:
            sql = query['sql']

            # Build date filter condition
            conditions = []
            if self.date_filter_start:
                conditions.append(f"report_date >= '{self.date_filter_start}'")
            if self.date_filter_end:
                conditions.append(f"report_date <= '{self.date_filter_end}'")

            date_condition = " AND ".join(conditions)

            # Inject WHERE clause into SQL
            # Handle both simple queries and queries with existing WHERE/GROUP BY/ORDER BY
            if 'WHERE' in sql.upper():
                # Already has WHERE clause, add to it
                modified_sql = sql.replace('WHERE', f'WHERE {date_condition} AND', 1)
            elif 'GROUP BY' in sql.upper():
                # No WHERE, but has GROUP BY - insert before GROUP BY
                modified_sql = sql.replace('GROUP BY', f'WHERE {date_condition} GROUP BY', 1)
            elif 'ORDER BY' in sql.upper():
                # No WHERE/GROUP BY, but has ORDER BY - insert before ORDER BY
                modified_sql = sql.replace('ORDER BY', f'WHERE {date_condition} ORDER BY', 1)
            else:
                # No WHERE/GROUP BY/ORDER BY - add at the end
                modified_sql = sql.strip() + f' WHERE {date_condition}'

            filtered_queries.append({
                **query,
                'sql': modified_sql
            })

        return filtered_queries

    def run_migration_acceptance(self):
        """
        Run migration acceptance validation

        Returns:
            Dictionary with acceptance status and details
        """
        print(f"\n{'='*80}")
        print("BUSINESS QUERY VALIDATION")
        print("=" * 80)
        print("Running business validation queries on both CDL and PRSTN...\n")

        # Load business queries
        business_queries = self.load_business_queries()

        # Run query validation
        query_results = self.validator.validate_query_results(questions=business_queries)

        # Separate critical vs non-critical results
        critical_results = []
        non_critical_results = []

        for i, result in enumerate(query_results):
            if business_queries and i < len(business_queries):
                is_critical = business_queries[i].get('critical', False)
                if is_critical:
                    critical_results.append(result)
                else:
                    non_critical_results.append(result)
            else:
                # Default queries are all considered critical
                critical_results.append(result)

        critical_passed = all(r.passed for r in critical_results)
        all_queries_passed = all(r.passed for r in query_results)

        # Generate acceptance report
        acceptance = {
            'timestamp': datetime.now().isoformat(),
            'cdl_table': self.cdl_table,
            'prstn_table': self.prstn_table,
            'tolerance': self.tolerance,
            'critical_queries_passed': critical_passed,
            'all_queries_passed': all_queries_passed,
            'total_queries': len(query_results),
            'critical_queries': len(critical_results),
            'critical_passed_count': sum(1 for r in critical_results if r.passed),
            'critical_failed_count': sum(1 for r in critical_results if not r.passed),
            'non_critical_queries': len(non_critical_results),
            'non_critical_passed_count': sum(1 for r in non_critical_results if r.passed),
            'non_critical_failed_count': sum(1 for r in non_critical_results if not r.passed),
            'query_results': query_results,
            'critical_results': critical_results,
            'non_critical_results': non_critical_results
        }

        # ACCEPTANCE DECISION
        # Migration is accepted if ALL critical queries pass
        acceptance['migration_accepted'] = critical_passed

        return acceptance

    def generate_acceptance_report(self, acceptance, output_file=None):
        """
        Generate migration acceptance report

        Args:
            acceptance: Acceptance validation results
            output_file: Optional output file prefix
        """
        print(f"\n{'='*80}")
        print("MIGRATION ACCEPTANCE DECISION")
        print("=" * 80)

        if acceptance['migration_accepted']:
            print("\n🎉 ✅ MIGRATION ACCEPTED ✅ 🎉")
            print("\nCDL produces equivalent results to PRSTN for all critical business queries.")
            print("CDL can SAFELY REPLACE PRSTN as the source of truth for subscription data.")
        else:
            print("\n❌ MIGRATION NOT ACCEPTED ❌")
            print("\nCDL does NOT produce equivalent results to PRSTN for all critical queries.")
            print("CDL CANNOT replace PRSTN until discrepancies are resolved.")

        print(f"\n{'='*80}")
        print("VALIDATION SUMMARY")
        print("=" * 80)

        print(f"\nTimestamp: {acceptance['timestamp']}")
        print(f"CDL Table: {acceptance['cdl_table']}")
        print(f"PRSTN Table: {acceptance['prstn_table']}")
        print(f"Tolerance: {acceptance['tolerance']}")

        print(f"\n{'='*80}")
        print("QUERY VALIDATION DETAILS")
        print("=" * 80)

        print(f"\nCritical Queries:")
        print(f"  Total: {acceptance['critical_queries']}")
        print(f"  ✅ Passed: {acceptance['critical_passed_count']}")
        print(f"  ❌ Failed: {acceptance['critical_failed_count']}")
        print(f"  Pass Rate: {acceptance['critical_passed_count']/acceptance['critical_queries']*100:.1f}%")

        if acceptance['non_critical_queries'] > 0:
            print(f"\nNon-Critical Queries:")
            print(f"  Total: {acceptance['non_critical_queries']}")
            print(f"  ✅ Passed: {acceptance['non_critical_passed_count']}")
            print(f"  ❌ Failed: {acceptance['non_critical_failed_count']}")
            print(f"  Pass Rate: {acceptance['non_critical_passed_count']/acceptance['non_critical_queries']*100:.1f}%")

        # Show failed critical queries
        if acceptance['critical_failed_count'] > 0:
            print(f"\n{'='*80}")
            print("❌ FAILED CRITICAL QUERIES")
            print("=" * 80)

            for result in acceptance['critical_results']:
                if not result.passed:
                    print(f"\n❌ {result.check_name}")
                    if result.error_message:
                        print(f"   Error: {result.error_message}")

                    # Show sample mismatches if available
                    if 'mismatches' in result.details:
                        mismatches = result.details['mismatches']
                        print(f"   Mismatches: {result.details.get('total_mismatches', len(mismatches))}")
                        print(f"   Sample (first 3):")
                        for m in mismatches[:3]:
                            if 'row' in m:
                                print(f"     Row {m['row']}, Col '{m['column']}': CDL={m.get('cdl_value')}, PRSTN={m.get('target_value')}")
                            else:
                                print(f"     {m}")

        # Show failed non-critical queries (if any)
        if acceptance['non_critical_failed_count'] > 0:
            print(f"\n{'='*80}")
            print("⚠️  FAILED NON-CRITICAL QUERIES (informational only)")
            print("=" * 80)

            for result in acceptance['non_critical_results']:
                if not result.passed:
                    print(f"\n⚠️  {result.check_name}")
                    if result.error_message:
                        print(f"   Error: {result.error_message}")

        # Save detailed report if output file specified
        if output_file:
            # Prepare data for Excel export
            excel_rows = []

            for result in acceptance['query_results']:
                row = {
                    'CDL table': acceptance['cdl_table'],
                    'PRSTN table': acceptance['prstn_table'],
                    'Metrics': result.check_name,
                    'CDL Value': result.details.get('cdl_value', ''),
                    'PRSTN Value': result.details.get('target_value', ''),
                    'Variance': result.details.get('delta', ''),
                    'Result': 'PASS' if result.passed else 'FAIL'
                }

                # Handle error cases where values might not be available
                if not result.passed and result.error_message:
                    row['CDL Value'] = result.error_message
                    row['PRSTN Value'] = ''
                    row['Variance'] = ''

                excel_rows.append(row)

            # Create DataFrame and save to Excel
            df = pd.DataFrame(excel_rows)
            excel_file = f"{output_file}.xlsx"
            df.to_excel(excel_file, index=False, sheet_name='Results')

            print(f"\n✅ Detailed report saved to: {excel_file}")

            # Also save JSON report for backward compatibility
            json_file = f"{output_file}.json"
            report_data = {
                'migration_accepted': acceptance['migration_accepted'],
                'timestamp': acceptance['timestamp'],
                'cdl_table': acceptance['cdl_table'],
                'prstn_table': acceptance['prstn_table'],
                'tolerance': acceptance['tolerance'],
                'summary': {
                    'critical_queries_passed': acceptance['critical_queries_passed'],
                    'all_queries_passed': acceptance['all_queries_passed']
                },
                'critical_queries': {
                    'total': acceptance['critical_queries'],
                    'passed': acceptance['critical_passed_count'],
                    'failed': acceptance['critical_failed_count'],
                    'pass_rate': acceptance['critical_passed_count']/acceptance['critical_queries']*100 if acceptance['critical_queries'] > 0 else 0
                },
                'failed_queries': [
                    {
                        'name': r.check_name,
                        'type': r.validation_type,
                        'error': r.error_message,
                        'details': r.details
                    }
                    for r in acceptance['critical_results'] if not r.passed
                ]
            }

            with open(json_file, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)

            print(f"✅ JSON report saved to: {json_file}")

        print(f"\n{'='*80}")

        if acceptance['migration_accepted']:
            print("✅ MIGRATION ACCEPTED - CDL is ready to replace PRSTN")
        else:
            print("❌ MIGRATION NOT ACCEPTED - Fix discrepancies before proceeding")

        print("=" * 80)

        return acceptance


def main():
    # Load configuration from file
    config = load_config()

    # Validate required parameters
    if not config.get('project_id'):
        print("❌ ERROR: 'project_id' is required in config.json")
        return 1
    if not config.get('cdl_table'):
        print("❌ ERROR: 'cdl_table' is required in config.json")
        return 1
    if not config.get('prstn_table'):
        print("❌ ERROR: 'prstn_table' is required in config.json")
        return 1

    # Initialize validator
    try:
        validator = MigrationAcceptanceValidator(
            project_id=config['project_id'],
            cdl_table=config['cdl_table'],
            prstn_table=config['prstn_table'],
            credentials_path=config.get('credentials_path'),
            tolerance=config.get('tolerance', 0.01),
            queries_file=config.get('queries_file', 'cdl_migration_acceptance_queries.json'),
            date_filter_start=config.get('date_filter_start'),
            date_filter_end=config.get('date_filter_end')
        )
    except Exception as e:
        print(f"\n❌ Failed to initialize validator: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Run migration acceptance validation
    try:
        acceptance = validator.run_migration_acceptance()
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Generate acceptance report
    try:
        output_file = config.get('output_file', 'cdl_migration_acceptance_report')
        validator.generate_acceptance_report(acceptance, output_file)
    except Exception as e:
        print(f"\n❌ Failed to generate report: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Return exit code
    if acceptance['migration_accepted']:
        return 0  # Success - migration accepted
    else:
        return 1  # Failure - migration not accepted


if __name__ == '__main__':
    sys.exit(main())
