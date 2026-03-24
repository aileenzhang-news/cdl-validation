"""
Table Comparison Validator for CDL

Validates data consistency between two BigQuery tables by:
1. Schema check - column names, types, nullability
2. Quantity check - row counts, distinct values, null rates
3. Query-result matching - run same queries on both tables and compare results
"""

from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from google.cloud import bigquery
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ValidationResult:
    """Container for validation results"""
    validation_type: str  # 'schema', 'quantity', 'query'
    check_name: str
    passed: bool
    details: Dict[str, Any]
    error_message: Optional[str] = None


class TableComparisonValidator:
    """
    Validates data consistency between CDL table and target table in BigQuery
    """

    def __init__(
        self,
        project_id: str = 'ncau-data-newsquery-sit',
        cdl_table: str = 'ncau-data-newsquery-sit.cdl.subscription_transaction_fct',
        target_table: str = 'ncau-data-newsquery-prd.prstn_consumer.subscription_base_movement_agg_snap',
        credentials_path: Optional[str] = None,
        tolerance: float = 0.01
    ):
        """
        Initialize table comparison validator

        Args:
            project_id: GCP project ID
            cdl_table: Full path to CDL table (project.dataset.table)
            target_table: Full path to target table (project.dataset.table)
            credentials_path: Optional path to service account JSON key
            tolerance: Numeric tolerance for comparisons (default: 0.01)
        """
        self.project_id = project_id
        self.cdl_table = cdl_table
        self.target_table = target_table
        self.tolerance = tolerance

        # Initialize BigQuery client
        if credentials_path:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=project_id
            )
        else:
            self.client = bigquery.Client(project=project_id)

        print(f"Initialized TableComparisonValidator")
        print(f"  CDL table: {cdl_table}")
        print(f"  Target table: {target_table}")
        print(f"  Project: {project_id}")

    def validate_all(self) -> List[ValidationResult]:
        """
        Run all validation checks

        Returns:
            List of ValidationResult objects
        """
        results = []

        print(f"\n{'='*80}")
        print("VALIDATION: Schema Check")
        print("=" * 80)
        results.extend(self.validate_schema())

        print(f"\n{'='*80}")
        print("VALIDATION: Quantity Check")
        print("=" * 80)
        results.extend(self.validate_quantities())

        print(f"\n{'='*80}")
        print("VALIDATION: Query-Result Matching")
        print("=" * 80)
        results.extend(self.validate_query_results())

        return results

    def validate_schema(self) -> List[ValidationResult]:
        """
        Validate schema consistency between CDL and target tables
        Checks: column names, data types, nullability

        Returns:
            List of ValidationResult objects
        """
        results = []

        # Get schemas for both tables
        cdl_schema = self._get_table_schema(self.cdl_table)
        target_schema = self._get_table_schema(self.target_table)

        print(f"\nCDL table columns: {len(cdl_schema)}")
        print(f"Target table columns: {len(target_schema)}")

        # Create lookup dictionaries
        cdl_cols = {row['column_name']: row for row in cdl_schema}
        target_cols = {row['column_name']: row for row in target_schema}

        # Check 1: Column name matching
        cdl_only = set(cdl_cols.keys()) - set(target_cols.keys())
        target_only = set(target_cols.keys()) - set(cdl_cols.keys())
        common_cols = set(cdl_cols.keys()) & set(target_cols.keys())

        results.append(ValidationResult(
            validation_type='schema',
            check_name='column_names',
            passed=(len(cdl_only) == 0 and len(target_only) == 0),
            details={
                'cdl_columns': len(cdl_cols),
                'target_columns': len(target_cols),
                'common_columns': len(common_cols),
                'cdl_only': list(cdl_only),
                'target_only': list(target_only)
            },
            error_message=f"Column mismatch: {len(cdl_only)} in CDL only, {len(target_only)} in target only" if (cdl_only or target_only) else None
        ))

        if cdl_only:
            print(f"\n⚠️  Columns only in CDL ({len(cdl_only)}): {', '.join(sorted(cdl_only)[:5])}{'...' if len(cdl_only) > 5 else ''}")
        if target_only:
            print(f"⚠️  Columns only in Target ({len(target_only)}): {', '.join(sorted(target_only)[:5])}{'...' if len(target_only) > 5 else ''}")

        # Check 2: Data type matching for common columns
        type_mismatches = []
        for col in common_cols:
            cdl_type = cdl_cols[col]['data_type']
            target_type = target_cols[col]['data_type']
            if cdl_type != target_type:
                type_mismatches.append({
                    'column': col,
                    'cdl_type': cdl_type,
                    'target_type': target_type
                })

        results.append(ValidationResult(
            validation_type='schema',
            check_name='data_types',
            passed=(len(type_mismatches) == 0),
            details={
                'common_columns_checked': len(common_cols),
                'type_mismatches': type_mismatches
            },
            error_message=f"{len(type_mismatches)} data type mismatches" if type_mismatches else None
        ))

        if type_mismatches:
            print(f"\n⚠️  Data type mismatches ({len(type_mismatches)}):")
            for mismatch in type_mismatches[:5]:
                print(f"    {mismatch['column']}: CDL={mismatch['cdl_type']}, Target={mismatch['target_type']}")

        # Check 3: Nullability matching for common columns
        nullability_mismatches = []
        for col in common_cols:
            cdl_nullable = cdl_cols[col]['is_nullable']
            target_nullable = target_cols[col]['is_nullable']
            if cdl_nullable != target_nullable:
                nullability_mismatches.append({
                    'column': col,
                    'cdl_nullable': cdl_nullable,
                    'target_nullable': target_nullable
                })

        results.append(ValidationResult(
            validation_type='schema',
            check_name='nullability',
            passed=(len(nullability_mismatches) == 0),
            details={
                'common_columns_checked': len(common_cols),
                'nullability_mismatches': nullability_mismatches
            },
            error_message=f"{len(nullability_mismatches)} nullability mismatches" if nullability_mismatches else None
        ))

        if nullability_mismatches:
            print(f"\n⚠️  Nullability mismatches ({len(nullability_mismatches)}):")
            for mismatch in nullability_mismatches[:5]:
                print(f"    {mismatch['column']}: CDL={mismatch['cdl_nullable']}, Target={mismatch['target_nullable']}")

        # Summary
        passed = sum(1 for r in results if r.passed)
        print(f"\n✓ Schema validation: {passed}/{len(results)} checks passed")

        return results

    def validate_quantities(self) -> List[ValidationResult]:
        """
        Validate data quantities between CDL and target tables
        Checks: row counts, distinct values, null rates

        Returns:
            List of ValidationResult objects
        """
        results = []

        # Check 1: Row counts
        cdl_count = self._get_row_count(self.cdl_table)
        target_count = self._get_row_count(self.target_table)

        print(f"\nCDL row count: {cdl_count:,}")
        print(f"Target row count: {target_count:,}")

        results.append(ValidationResult(
            validation_type='quantity',
            check_name='row_count',
            passed=(cdl_count == target_count),
            details={
                'cdl_count': cdl_count,
                'target_count': target_count,
                'difference': abs(cdl_count - target_count),
                'percent_diff': abs(cdl_count - target_count) / max(cdl_count, 1) * 100
            },
            error_message=f"Row count mismatch: CDL={cdl_count:,}, Target={target_count:,}" if cdl_count != target_count else None
        ))

        # Check 2: Distinct value counts for key columns
        # Get common columns between both tables
        cdl_schema = self._get_table_schema(self.cdl_table)
        target_schema = self._get_table_schema(self.target_table)
        cdl_cols = {row['column_name'] for row in cdl_schema}
        target_cols = {row['column_name'] for row in target_schema}
        common_cols = list(cdl_cols & target_cols)

        # Sample key columns for distinct count check (limit to avoid long queries)
        key_columns = [col for col in common_cols if col in [
            'report_date', 'masthead', 'classification_level_1', 'offer_category_name',
            'delivery_medium_type', 'dw_source_system_code'
        ]]

        distinct_mismatches = []
        for col in key_columns:
            cdl_distinct = self._get_distinct_count(self.cdl_table, col)
            target_distinct = self._get_distinct_count(self.target_table, col)

            if cdl_distinct != target_distinct:
                distinct_mismatches.append({
                    'column': col,
                    'cdl_distinct': cdl_distinct,
                    'target_distinct': target_distinct,
                    'difference': abs(cdl_distinct - target_distinct)
                })

        results.append(ValidationResult(
            validation_type='quantity',
            check_name='distinct_counts',
            passed=(len(distinct_mismatches) == 0),
            details={
                'columns_checked': key_columns,
                'mismatches': distinct_mismatches
            },
            error_message=f"{len(distinct_mismatches)} distinct count mismatches" if distinct_mismatches else None
        ))

        if distinct_mismatches:
            print(f"\n⚠️  Distinct count mismatches ({len(distinct_mismatches)}):")
            for mismatch in distinct_mismatches:
                print(f"    {mismatch['column']}: CDL={mismatch['cdl_distinct']:,}, Target={mismatch['target_distinct']:,}")

        # Check 3: Null rates for key columns
        null_rate_mismatches = []
        for col in key_columns:
            cdl_null_rate = self._get_null_rate(self.cdl_table, col)
            target_null_rate = self._get_null_rate(self.target_table, col)

            # Allow small differences in null rates (within tolerance)
            if abs(cdl_null_rate - target_null_rate) > self.tolerance:
                null_rate_mismatches.append({
                    'column': col,
                    'cdl_null_rate': cdl_null_rate,
                    'target_null_rate': target_null_rate,
                    'difference': abs(cdl_null_rate - target_null_rate)
                })

        results.append(ValidationResult(
            validation_type='quantity',
            check_name='null_rates',
            passed=(len(null_rate_mismatches) == 0),
            details={
                'columns_checked': key_columns,
                'tolerance': self.tolerance,
                'mismatches': null_rate_mismatches
            },
            error_message=f"{len(null_rate_mismatches)} null rate mismatches" if null_rate_mismatches else None
        ))

        if null_rate_mismatches:
            print(f"\n⚠️  Null rate mismatches ({len(null_rate_mismatches)}):")
            for mismatch in null_rate_mismatches:
                print(f"    {mismatch['column']}: CDL={mismatch['cdl_null_rate']:.2%}, Target={mismatch['target_null_rate']:.2%}")

        # Summary
        passed = sum(1 for r in results if r.passed)
        print(f"\n✓ Quantity validation: {passed}/{len(results)} checks passed")

        return results

    def validate_query_results(self, questions: Optional[List[Dict[str, str]]] = None) -> List[ValidationResult]:
        """
        Validate query results between CDL and target tables
        Runs same queries on both tables and compares results

        Args:
            questions: List of dicts with 'name' and 'sql' keys
                      If None, uses default set of validation queries

        Returns:
            List of ValidationResult objects
        """
        results = []

        # Default validation queries if none provided
        if questions is None:
            questions = self._get_default_validation_queries()

        print(f"\nRunning {len(questions)} validation queries...")

        for i, question in enumerate(questions, 1):
            name = question['name']
            sql_template = question['sql']

            print(f"\n  [{i}/{len(questions)}] {name}")

            try:
                # Run query on CDL table
                cdl_query = sql_template.format(table=self.cdl_table)
                cdl_result = self.client.query(cdl_query).to_dataframe()

                # Run query on target table
                target_query = sql_template.format(table=self.target_table)
                target_result = self.client.query(target_query).to_dataframe()

                # Compare results
                match, details = self._compare_query_results(cdl_result, target_result, name)

                results.append(ValidationResult(
                    validation_type='query',
                    check_name=name,
                    passed=match,
                    details=details,
                    error_message=None if match else details.get('error', 'Results do not match')
                ))

                if match:
                    print(f"    ✓ PASS")
                else:
                    print(f"    ✗ FAIL: {details.get('error', 'Results do not match')}")

            except Exception as e:
                results.append(ValidationResult(
                    validation_type='query',
                    check_name=name,
                    passed=False,
                    details={'error': str(e)},
                    error_message=f"Query execution failed: {str(e)}"
                ))
                print(f"    ✗ ERROR: {str(e)}")

        # Summary
        passed = sum(1 for r in results if r.passed)
        print(f"\n✓ Query validation: {passed}/{len(results)} queries passed")

        return results

    def _get_default_validation_queries(self) -> List[Dict[str, str]]:
        """
        Get default set of validation queries

        Returns:
            List of query definitions
        """
        return [
            {
                'name': 'total_subscription_count',
                'sql': 'SELECT SUM(subscription_count) as total FROM `{table}`'
            },
            {
                'name': 'total_acquisition_count',
                'sql': 'SELECT SUM(acquisition_count) as total FROM `{table}`'
            },
            {
                'name': 'total_cancellation_count',
                'sql': 'SELECT SUM(cancellation_count) as total FROM `{table}`'
            },
            {
                'name': 'subscription_by_masthead',
                'sql': '''
                    SELECT
                        masthead,
                        SUM(subscription_count) as total_subscriptions
                    FROM `{table}`
                    GROUP BY masthead
                    ORDER BY masthead
                '''
            },
            {
                'name': 'subscription_by_date',
                'sql': '''
                    SELECT
                        report_date,
                        SUM(subscription_count) as total_subscriptions
                    FROM `{table}`
                    GROUP BY report_date
                    ORDER BY report_date
                '''
            },
            {
                'name': 'acquisition_by_classification',
                'sql': '''
                    SELECT
                        classification_level_1,
                        SUM(acquisition_count) as total_acquisitions
                    FROM `{table}`
                    GROUP BY classification_level_1
                    ORDER BY classification_level_1
                '''
            },
            {
                'name': 'subscription_by_delivery_medium',
                'sql': '''
                    SELECT
                        delivery_medium_type,
                        SUM(subscription_count) as total_subscriptions
                    FROM `{table}`
                    GROUP BY delivery_medium_type
                    ORDER BY delivery_medium_type
                '''
            }
        ]

    def _get_table_schema(self, table_path: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table

        Args:
            table_path: Full table path (project.dataset.table)

        Returns:
            List of column metadata dicts
        """
        parts = table_path.split('.')
        if len(parts) != 3:
            raise ValueError(f"Invalid table path: {table_path}. Expected format: project.dataset.table")

        project, dataset, table = parts

        query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
        """

        df = self.client.query(query).to_dataframe()
        return df.to_dict('records')

    def _get_row_count(self, table_path: str) -> int:
        """Get total row count for a table"""
        query = f"SELECT COUNT(*) as count FROM `{table_path}`"
        result = self.client.query(query).to_dataframe()
        return int(result['count'].iloc[0])

    def _get_distinct_count(self, table_path: str, column: str) -> int:
        """Get distinct value count for a column"""
        query = f"SELECT COUNT(DISTINCT {column}) as count FROM `{table_path}`"
        result = self.client.query(query).to_dataframe()
        return int(result['count'].iloc[0])

    def _get_null_rate(self, table_path: str, column: str) -> float:
        """Get null rate (percentage) for a column"""
        query = f"""
        SELECT
            COUNTIF({column} IS NULL) / COUNT(*) as null_rate
        FROM `{table_path}`
        """
        result = self.client.query(query).to_dataframe()
        return float(result['null_rate'].iloc[0])

    def _compare_query_results(
        self,
        cdl_result: pd.DataFrame,
        target_result: pd.DataFrame,
        query_name: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Compare query results from CDL and target tables

        Args:
            cdl_result: Query result from CDL table
            target_result: Query result from target table
            query_name: Name of the query being compared

        Returns:
            Tuple of (match_status, details_dict)
        """
        details = {
            'cdl_rows': len(cdl_result),
            'target_rows': len(target_result),
            'cdl_columns': list(cdl_result.columns),
            'target_columns': list(target_result.columns)
        }

        # Check row counts
        if len(cdl_result) != len(target_result):
            details['error'] = f"Row count mismatch: CDL={len(cdl_result)}, Target={len(target_result)}"
            return False, details

        # Check column names
        if list(cdl_result.columns) != list(target_result.columns):
            details['error'] = f"Column mismatch"
            return False, details

        # For single aggregate values, compare directly
        if len(cdl_result) == 1 and len(cdl_result.columns) == 1:
            cdl_val = cdl_result.iloc[0, 0]
            target_val = target_result.iloc[0, 0]

            # Handle NaN/None
            if pd.isna(cdl_val) and pd.isna(target_val):
                details['cdl_value'] = None
                details['target_value'] = None
                return True, details

            # Numeric comparison with tolerance
            if isinstance(cdl_val, (int, float)) and isinstance(target_val, (int, float)):
                delta = abs(cdl_val - target_val)
                match = delta <= self.tolerance

                details['cdl_value'] = float(cdl_val)
                details['target_value'] = float(target_val)
                details['delta'] = float(delta)

                if not match:
                    details['error'] = f"Value mismatch: CDL={cdl_val}, Target={target_val}, Delta={delta}"

                return match, details

        # For grouped results, compare row by row
        # Sort both dataframes by all columns to ensure same order
        sort_cols = list(cdl_result.columns)
        cdl_sorted = cdl_result.sort_values(by=sort_cols).reset_index(drop=True)
        target_sorted = target_result.sort_values(by=sort_cols).reset_index(drop=True)

        # Compare values
        mismatches = []
        for idx in range(len(cdl_sorted)):
            for col in cdl_sorted.columns:
                cdl_val = cdl_sorted.loc[idx, col]
                target_val = target_sorted.loc[idx, col]

                # Handle NaN/None
                if pd.isna(cdl_val) and pd.isna(target_val):
                    continue

                # Numeric comparison with tolerance
                if isinstance(cdl_val, (int, float)) and isinstance(target_val, (int, float)):
                    if abs(cdl_val - target_val) > self.tolerance:
                        mismatches.append({
                            'row': idx,
                            'column': col,
                            'cdl_value': float(cdl_val),
                            'target_value': float(target_val),
                            'delta': float(abs(cdl_val - target_val))
                        })
                # String comparison
                elif cdl_val != target_val:
                    mismatches.append({
                        'row': idx,
                        'column': col,
                        'cdl_value': str(cdl_val),
                        'target_value': str(target_val)
                    })

        if mismatches:
            details['mismatches'] = mismatches[:10]  # Limit to first 10
            details['total_mismatches'] = len(mismatches)
            details['error'] = f"{len(mismatches)} value mismatches found"
            return False, details

        return True, details

    def generate_report(self, results: List[ValidationResult], output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate summary report from validation results

        Args:
            results: List of ValidationResult objects
            output_file: Optional output file prefix (will create .csv and .txt)

        Returns:
            Summary dictionary
        """
        # Calculate summary statistics
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed

        by_type = {}
        for r in results:
            if r.validation_type not in by_type:
                by_type[r.validation_type] = {'total': 0, 'passed': 0, 'failed': 0}
            by_type[r.validation_type]['total'] += 1
            if r.passed:
                by_type[r.validation_type]['passed'] += 1
            else:
                by_type[r.validation_type]['failed'] += 1

        summary = {
            'timestamp': datetime.now().isoformat(),
            'cdl_table': self.cdl_table,
            'target_table': self.target_table,
            'total': total,
            'passed': passed,
            'failed': failed,
            'pass_rate': (passed / total * 100) if total > 0 else 0,
            'by_type': by_type
        }

        # Print summary
        print(f"\n{'='*80}")
        print("VALIDATION SUMMARY")
        print("=" * 80)
        print(f"\nTotal validations: {total}")
        print(f"  ✅ Passed: {passed}")
        print(f"  ❌ Failed: {failed}")
        print(f"  Pass rate: {summary['pass_rate']:.1f}%")

        print(f"\nBy validation type:")
        for vtype, stats in by_type.items():
            print(f"  {vtype}: {stats['passed']}/{stats['total']} passed")

        # Save detailed results to CSV if output file specified
        if output_file:
            # Convert results to DataFrame
            records = []
            for r in results:
                record = {
                    'validation_type': r.validation_type,
                    'check_name': r.check_name,
                    'passed': r.passed,
                    'error_message': r.error_message or ''
                }
                # Add key details
                if 'cdl_count' in r.details:
                    record['cdl_value'] = r.details['cdl_count']
                    record['target_value'] = r.details.get('target_count')
                elif 'cdl_value' in r.details:
                    record['cdl_value'] = r.details['cdl_value']
                    record['target_value'] = r.details.get('target_value')

                records.append(record)

            df = pd.DataFrame(records)
            csv_file = f"{output_file}.csv"
            df.to_csv(csv_file, index=False)
            print(f"\n✅ Detailed results saved to: {csv_file}")

            # Save text summary
            txt_file = f"{output_file}.txt"
            with open(txt_file, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("CDL TABLE COMPARISON VALIDATION REPORT\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Timestamp: {summary['timestamp']}\n")
                f.write(f"CDL Table: {self.cdl_table}\n")
                f.write(f"Target Table: {self.target_table}\n\n")
                f.write(f"Total Validations: {total}\n")
                f.write(f"  ✅ Passed: {passed}\n")
                f.write(f"  ❌ Failed: {failed}\n")
                f.write(f"  Pass Rate: {summary['pass_rate']:.1f}%\n\n")

                f.write("By Validation Type:\n")
                for vtype, stats in by_type.items():
                    f.write(f"  {vtype}: {stats['passed']}/{stats['total']} passed\n")

                f.write("\n" + "=" * 80 + "\n")
                f.write("FAILED CHECKS\n")
                f.write("=" * 80 + "\n\n")

                for r in results:
                    if not r.passed:
                        f.write(f"\n{r.validation_type}.{r.check_name}:\n")
                        f.write(f"  Error: {r.error_message}\n")
                        if 'mismatches' in r.details:
                            f.write(f"  Showing first few mismatches:\n")
                            for m in r.details['mismatches'][:5]:
                                f.write(f"    {m}\n")

            print(f"✅ Summary report saved to: {txt_file}")

        return summary
