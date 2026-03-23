#!/usr/bin/env python3
"""
Local Source Validator

Validates CDL against source data from either:
- Local CSV exports from BigQuery source tables (default)
- Direct BigQuery connection (optional)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re


@dataclass
class ValidationResult:
    """Result of a single column validation"""
    column_name: str
    cte_name: str
    transformation_type: str
    passed: bool
    total_rows: int
    mismatched_rows: int = 0
    max_delta: float = 0.0
    error_message: str = ""
    sample_mismatches: Optional[pd.DataFrame] = None


class LocalSourceValidator:
    """
    Validates CDL by reconstructing transformations from mapping.xlsx
    using local CSV exports of source tables.
    """

    def __init__(
        self,
        mapping_file: str,
        source_data_dir: Optional[str] = None,
        tolerance: float = 0.01,
        use_bigquery: bool = False,
        bigquery_project: str = 'ncau-data-newsquery-prd',
        bigquery_credentials: Optional[str] = None,
        start_date: str = '2000-03-30',
        end_date: str = '2026-03-16'
    ):
        """
        Initialize validator

        Args:
            mapping_file: Path to mapping.xlsx
            source_data_dir: Directory containing source CSV files (required if use_bigquery=False)
            tolerance: Numeric tolerance for value comparison
            use_bigquery: If True, load data from BigQuery instead of local CSVs
            bigquery_project: GCP project ID (only used if use_bigquery=True)
            bigquery_credentials: Path to GCP service account JSON (optional, uses ADC if not provided)
            start_date: Start date for BigQuery data filtering (YYYY-MM-DD)
            end_date: End date for BigQuery data filtering (YYYY-MM-DD)
        """
        self.mapping_file = Path(mapping_file)
        self.source_data_dir = Path(source_data_dir) if source_data_dir else None
        self.tolerance = tolerance
        self.use_bigquery = use_bigquery
        self.bigquery_project = bigquery_project
        self.bigquery_credentials = bigquery_credentials
        self.start_date = start_date
        self.end_date = end_date

        # Validate inputs
        if not use_bigquery and not source_data_dir:
            raise ValueError("source_data_dir is required when use_bigquery=False")

        # Load mapping configuration
        self._load_mapping()

        # Load source data
        self._load_source_data()

        # CTEs storage
        self.ctes = {}

    def _load_mapping(self):
        """Load all sheets from mapping.xlsx"""
        print(f"Loading mapping from {self.mapping_file}...")

        self.mapping = {
            'cte_structure': pd.read_excel(self.mapping_file, sheet_name='01_CTE_Structure'),
            'source_tables': pd.read_excel(self.mapping_file, sheet_name='02_Source_Tables'),
            'column_mappings': pd.read_excel(self.mapping_file, sheet_name='03_Column_Mappings'),
            'join_logic': pd.read_excel(self.mapping_file, sheet_name='04_Join_Logic'),
            'filter_logic': pd.read_excel(self.mapping_file, sheet_name='05_Filter_Logic'),
            'aggregation_rules': pd.read_excel(self.mapping_file, sheet_name='06_Aggregation_Rules'),
            'derived_metrics': pd.read_excel(self.mapping_file, sheet_name='07_Derived_Metrics'),
        }

        print(f"  ✓ Loaded {len(self.mapping['column_mappings'])} column mappings")
        print(f"  ✓ Loaded {len(self.mapping['cte_structure'])} CTEs")

    def _load_source_data(self):
        """Load source data from either BigQuery or local CSV files"""

        if self.use_bigquery:
            # Load from BigQuery
            from validators.bq_data_loader import BigQueryDataLoader

            loader = BigQueryDataLoader(
                project_id=self.bigquery_project,
                credentials_path=self.bigquery_credentials,
                start_date=self.start_date,
                end_date=self.end_date
            )

            self.source_data = loader.load_all_source_data()

        else:
            # Load from local CSV files
            print(f"\nLoading source data from {self.source_data_dir}...")

            self.source_data = {}

            # Load calendar
            cal_file = self.source_data_dir / 'source_calendar.csv'
            if cal_file.exists():
                self.source_data['cal'] = pd.read_csv(cal_file)
                # Handle different column names
                if 'calendar_date' in self.source_data['cal'].columns:
                    self.source_data['cal']['report_date'] = pd.to_datetime(self.source_data['cal']['calendar_date'])
                elif 'report_date' in self.source_data['cal'].columns:
                    self.source_data['cal']['report_date'] = pd.to_datetime(self.source_data['cal']['report_date'])
                print(f"  ✓ Calendar: {len(self.source_data['cal']):,} rows")

            # Load subscription base
            sbf_file = self.source_data_dir / 'source_subscription_base.csv'
            if sbf_file.exists():
                self.source_data['sbf'] = pd.read_csv(sbf_file)
                # Convert datetime columns
                for col in ['dw_effective_start_datetime', 'dw_effective_end_datetime', 'subscription_tenure_start_datetime']:
                    if col in self.source_data['sbf'].columns:
                        self.source_data['sbf'][col] = pd.to_datetime(self.source_data['sbf'][col], errors='coerce')
                print(f"  ✓ Subscription Base: {len(self.source_data['sbf']):,} rows")

            # Load subscription movement
            smf_file = self.source_data_dir / 'source_subscription_movement_partial.csv'
            if smf_file.exists():
                self.source_data['smf'] = pd.read_csv(smf_file)
                # Convert datetime columns
                for col in ['report_date', 'movement_datetime', 'subscription_tenure_start_datetime']:
                    if col in self.source_data['smf'].columns:
                        self.source_data['smf'][col] = pd.to_datetime(self.source_data['smf'][col], errors='coerce')
                print(f"  ✓ Subscription Movement: {len(self.source_data['smf']):,} rows")

            # Load schemas
            schema_file = self.source_data_dir / 'source_schemas.csv'
            if schema_file.exists():
                self.source_data['schemas'] = pd.read_csv(schema_file)
                print(f"  ✓ Schemas: {len(self.source_data['schemas']):,} column definitions")

    def validate_schema(self) -> Dict:
        """
        Layer 1: Validate that all required columns exist in source tables

        Returns:
            Dict with validation results
        """
        print("\n" + "=" * 80)
        print("LAYER 1: SOURCE SCHEMA VALIDATION")
        print("=" * 80)

        if 'schemas' not in self.source_data:
            return {'status': 'error', 'message': 'Schema file not found'}

        schemas_df = self.source_data['schemas']
        results = {'missing_columns': [], 'validated_tables': {}}

        # Map source table names
        table_map = {
            'sbf': 'subscription_base_extended_fct',
            'smf': 'subscription_movement_extended_fct',
            'cal': 'v_calendar_dim'
        }

        # Check each source table
        for alias, table_name in table_map.items():
            print(f"\nChecking {alias}: {table_name}")

            # Get columns from schema
            table_schema = schemas_df[schemas_df['table_name'] == table_name]
            available_columns = set(table_schema['column_name'].str.lower())

            # Get required columns from mapping
            pattern = rf'{alias}\.([a-zA-Z_][a-zA-Z0-9_]*)'
            all_expressions = self.mapping['column_mappings']['source_expression'].fillna('') + ' ' + \
                            self.mapping['column_mappings']['depends_on'].fillna('')

            required_columns = set()
            for expr in all_expressions:
                matches = re.findall(pattern, str(expr))
                required_columns.update([m.lower() for m in matches])

            # Find missing columns
            missing = required_columns - available_columns

            results['validated_tables'][alias] = {
                'table_name': table_name,
                'available_columns': len(available_columns),
                'required_columns': len(required_columns),
                'missing_columns': list(missing)
            }

            if missing:
                print(f"  ❌ Missing {len(missing)} columns:")
                for col in sorted(missing):
                    print(f"     - {col}")
                    results['missing_columns'].append(f"{table_name}.{col}")
            else:
                print(f"  ✅ All {len(required_columns)} required columns found")

        # Summary
        if results['missing_columns']:
            print(f"\n❌ Schema validation FAILED")
            print(f"   Missing {len(results['missing_columns'])} columns total")
            results['status'] = 'failed'
        else:
            print(f"\n✅ Schema validation PASSED")
            results['status'] = 'passed'

        return results

    def build_cte(self, cte_name: str) -> pd.DataFrame:
        """
        Build a CTE by applying transformations from mapping.xlsx

        Args:
            cte_name: Name of CTE to build (e.g., 'sba_inner', 'sma', 'final_select')

        Returns:
            DataFrame with CTE results
        """
        # Check if already built
        if cte_name in self.ctes:
            return self.ctes[cte_name]

        print(f"\nBuilding CTE: {cte_name}")

        # Handle base CTEs (direct from source tables)
        if cte_name == 'cal':
            self.ctes['cal'] = self.source_data['cal'].copy()
            return self.ctes['cal']

        if cte_name == 'sbf':
            self.ctes['sbf'] = self.source_data['sbf'].copy()
            return self.ctes['sbf']

        if cte_name == 'smf':
            self.ctes['smf'] = self.source_data['smf'].copy()
            return self.ctes['smf']

        # Check if this is a UNION CTE
        cte_info = self.mapping['cte_structure'][self.mapping['cte_structure']['cte_name'] == cte_name]
        if len(cte_info) > 0 and cte_info.iloc[0]['cte_type'] == 'union':
            # Handle UNION (e.g., sbm = sba UNION ALL sma)
            parent_cte = cte_info.iloc[0]['parent_cte']
            parents = [p.strip() for p in str(parent_cte).split(',')]

            print(f"  UNION of: {', '.join(parents)}")

            # Build parent CTEs
            dfs = []
            for parent in parents:
                parent_df = self.build_cte(parent)
                dfs.append(parent_df)

            # Union all DataFrames
            result = pd.concat(dfs, ignore_index=True)

            self.ctes[cte_name] = result
            print(f"  ✓ Built {cte_name}: {len(result):,} rows, {len(result.columns)} columns")
            return result

        # Get join logic for this CTE
        join_info = self.mapping['join_logic'][self.mapping['join_logic']['cte_name'] == cte_name]

        if len(join_info) > 0:
            # This CTE is created via JOIN
            join_row = join_info.iloc[0]
            left_table = join_row['left_table']
            right_table = join_row['right_table']
            join_type = join_row['join_type']
            join_condition = join_row['join_condition']

            # Build dependencies first
            left_df = self.build_cte(left_table)
            right_df = self.build_cte(right_table)

            # Apply JOIN
            result = self._apply_join(left_df, right_df, left_table, right_table, join_type, join_condition)

        else:
            # This CTE transforms another CTE
            # Find parent CTE from column mappings
            parent_cte = self._find_parent_cte(cte_name)
            result = self.build_cte(parent_cte).copy()

        # Apply column transformations for this CTE
        result = self._apply_transformations(result, cte_name)

        self.ctes[cte_name] = result
        print(f"  ✓ Built {cte_name}: {len(result):,} rows, {len(result.columns)} columns")

        return result

    def _apply_join(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_name: str,
        right_name: str,
        join_type: str,
        join_condition: str
    ) -> pd.DataFrame:
        """Apply JOIN between two DataFrames"""

        # Handle BETWEEN join (special case for date ranges)
        if 'between' in join_condition.lower():
            # Example: cal.report_date between cast(sbf.dw_effective_start_datetime as date) and cast(sbf.dw_effective_end_datetime as date)

            # For sba_inner: JOIN sbf with cal where report_date is between start and end dates
            if left_name == 'sbf' and right_name == 'cal':
                result = pd.merge(
                    left_df.assign(key=1),
                    right_df.assign(key=1),
                    on='key'
                ).drop('key', axis=1)

                # Filter: cal.report_date BETWEEN sbf start and end
                mask = (
                    (result['report_date'] >= result['dw_effective_start_datetime'].dt.date) &
                    (result['report_date'] <= result['dw_effective_end_datetime'].dt.date)
                )
                result = result[mask]

                return result

        # Handle equality join
        elif '=' in join_condition:
            # Example: smf.report_date = cal.report_date
            # Extract column names
            parts = join_condition.split('=')
            left_col = parts[0].strip().split('.')[-1]
            right_col = parts[1].strip().split('.')[-1]

            if join_type == 'INNER JOIN':
                return pd.merge(left_df, right_df, left_on=left_col, right_on=right_col, how='inner', suffixes=('', '_cal'))

        # Fallback: cross join
        return pd.merge(
            left_df.assign(key=1),
            right_df.assign(key=1),
            on='key'
        ).drop('key', axis=1)

    def _find_parent_cte(self, cte_name: str) -> str:
        """Find which CTE this one depends on"""
        # For aggregation CTEs, the parent is the _inner version
        if cte_name == 'sba':
            return 'sba_inner'
        if cte_name == 'sma':
            return 'sma_inner'
        if cte_name == 'sbm':
            # sbm merges sba and sma
            return 'sba'  # We'll handle sma separately
        if cte_name == 'final_select':
            return 'sbm'

        return cte_name

    def _apply_filters(self, df: pd.DataFrame, cte_name: str) -> pd.DataFrame:
        """Apply WHERE filters for a CTE based on mapping.xlsx filter logic"""

        # Get filter logic for this CTE
        filter_rules = self.mapping['filter_logic']
        cte_filters = filter_rules[filter_rules['cte_name'] == cte_name]

        if len(cte_filters) == 0:
            return df

        # Apply each filter
        for _, filter_row in cte_filters.iterrows():
            filter_expr = str(filter_row['filter_expression'])
            filter_type = str(filter_row['filter_type'])

            if filter_type != 'WHERE' or pd.isna(filter_expr) or filter_expr == 'nan':
                continue

            # Parse the filter expression
            # Handle: concat(dw_source_system_code, dw_billing_system_code) <> 'THINKSWG'
            if 'concat' in filter_expr.lower() and '<>' in filter_expr:
                # Extract the columns and excluded values
                # Pattern: concat(col1, col2) <> 'value' AND concat(col1, col2) <> 'value2'

                # Check if columns exist
                if 'dw_source_system_code' in df.columns and 'dw_billing_system_code' in df.columns:
                    original_count = len(df)

                    # Create concat column
                    df['_concat_code'] = df['dw_source_system_code'].astype(str) + df['dw_billing_system_code'].astype(str)

                    # Apply filter: exclude THINKSWG and THINKGPLA
                    df = df[~df['_concat_code'].isin(['THINKSWG', 'THINKGPLA'])].copy()

                    # Drop temp column
                    df = df.drop('_concat_code', axis=1)

                    filtered_count = len(df)
                    if filtered_count < original_count:
                        print(f"  Filtered {cte_name}: {original_count:,} → {filtered_count:,} rows (removed {original_count - filtered_count:,} THINKSWG/GPLA)")

        return df

    def _apply_transformations(self, df: pd.DataFrame, cte_name: str) -> pd.DataFrame:
        """Apply column transformations for a CTE"""

        # Get columns for this CTE
        cte_columns = self.mapping['column_mappings'][
            self.mapping['column_mappings']['cte_name'] == cte_name
        ]

        if len(cte_columns) == 0:
            return df

        # Step 1: Calculate subscription_tenure_days if needed
        if 'report_date' in df.columns and 'subscription_tenure_start_datetime' in df.columns:
            if 'subscription_tenure_days' not in df.columns:
                # Calculate tenure in days
                df['subscription_tenure_days'] = (
                    pd.to_datetime(df['report_date']) -
                    pd.to_datetime(df['subscription_tenure_start_datetime'])
                ).dt.days

        # Step 2: Apply filters (before other transformations)
        df = self._apply_filters(df, cte_name)

        # Step 3: Apply medium and complex type transformations (before aggregation)
        df = self._apply_column_transformations(df, cte_name, cte_columns)

        # Step 4: Check if this CTE needs aggregation
        agg_rules = self.mapping['aggregation_rules']
        needs_agg = cte_name in agg_rules['cte_name'].values

        if needs_agg:
            # Apply aggregation
            print(f"  Applying aggregation for {cte_name}...")
            df = self._apply_aggregation(df, cte_name, cte_columns)

        return df

    def _apply_column_transformations(
        self,
        df: pd.DataFrame,
        cte_name: str,
        cte_columns: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply medium and complex column transformations

        Args:
            df: Input DataFrame
            cte_name: Current CTE name
            cte_columns: Column mapping definitions for this CTE

        Returns:
            DataFrame with transformed columns added
        """

        # Get medium and complex columns
        transform_cols = cte_columns[
            cte_columns['transformation_type'].isin(['medium', 'complex'])
        ]

        if len(transform_cols) == 0:
            return df

        print(f"    Applying {len(transform_cols)} medium/complex transformations...")

        for _, col_info in transform_cols.iterrows():
            col_name = col_info['column_name']
            source_expr = str(col_info['source_expression'])
            transform_type = col_info['transformation_type']

            # Skip if column already exists
            if col_name in df.columns:
                continue

            try:
                if transform_type == 'medium':
                    # Handle EXTRACT and other medium transformations
                    df[col_name] = self._apply_medium_transformation(df, source_expr, col_name)
                elif transform_type == 'complex':
                    # Handle CASE WHEN transformations
                    df[col_name] = self._apply_complex_transformation(df, source_expr, col_name)
            except Exception as e:
                print(f"      ⚠️  Failed to transform {col_name}: {e}")
                # Set to None/default value
                df[col_name] = None

        return df

    def _apply_medium_transformation(
        self,
        df: pd.DataFrame,
        source_expr: str,
        col_name: str
    ):
        """
        Apply medium type transformations (EXTRACT, etc.)

        Args:
            df: Input DataFrame
            source_expr: SQL expression like "extract(DAYOFWEEK from sba.report_date)"
            col_name: Target column name

        Returns:
            Series with transformed values
        """

        expr = source_expr.lower().strip()

        # Pattern: extract(DAYOFWEEK from column_name)
        if 'extract' in expr and 'dayofweek' in expr:
            # Extract column name
            # Example: "extract(dayofweek from sba.report_date)" or "case when extract(dayofweek from sba.report_date) = 1 then 'Y' else 'N' end"

            # Find the column name between "from" and ")"
            import re
            match = re.search(r'from\s+(\w+\.)?(\w+)', expr)
            if match:
                date_col = match.group(2)

                if date_col in df.columns:
                    # Get day of week (1=Sunday, 7=Saturday in pandas, matching SQL DAYOFWEEK)
                    dow = pd.to_datetime(df[date_col]).dt.dayofweek + 1
                    # pandas: 0=Monday, need to shift to 1=Sunday
                    dow = dow.replace(7, 0) + 1

                    # Check if this is wrapped in a CASE WHEN for sunday_flag
                    if 'case when' in expr and "= 1 then 'y'" in expr:
                        # sunday_flag: 'Y' if Sunday (1), else 'N'
                        return dow.apply(lambda x: 'Y' if x == 1 else 'N')
                    else:
                        return dow

        # Default: return None
        return None

    def _apply_complex_transformation(
        self,
        df: pd.DataFrame,
        source_expr: str,
        col_name: str
    ):
        """
        Apply complex type transformations (CASE WHEN)

        Args:
            df: Input DataFrame
            source_expr: SQL CASE WHEN expression
            col_name: Target column name

        Returns:
            Series with transformed values
        """

        expr = source_expr.lower().strip()

        # tenure_group: CASE WHEN on subscription_tenure_days
        if col_name == 'tenure_group' and 'subscription_tenure_days' in df.columns:
            tenure = df['subscription_tenure_days']
            return pd.cut(
                tenure,
                bins=[-float('inf'), 0, 180, 360, 540, 720, float('inf')],
                labels=['CHECK', '0-180', '181-360', '361-540', '541-720', '721+'],
                include_lowest=True
            ).astype(str)

        # tenure_group_finance: CASE WHEN on subscription_tenure_days
        elif col_name == 'tenure_group_finance' and 'subscription_tenure_days' in df.columns:
            tenure = df['subscription_tenure_days']

            def categorize_tenure(days):
                if pd.isna(days) or days < 0:
                    return 'CHECK'
                elif days <= 182:
                    return '0-6months'
                elif days <= 365:
                    return '6-12months'
                elif days <= 365 * 2:
                    return '1-2years'
                elif days <= 365 * 3:
                    return '2-3years'
                elif days <= 365 * 4:
                    return '3-4years'
                elif days <= 365 * 5:
                    return '4-5years'
                elif days <= 365 * 6:
                    return '5-6years'
                elif days <= 365 * 7:
                    return '6-7years'
                else:
                    return '7years+'

            return tenure.apply(categorize_tenure)

        # Default: return None
        return None

    def _parse_aggregation_expression(
        self,
        source_expr: str,
        df: pd.DataFrame,
        parent_cte: str
    ) -> tuple:
        """
        Parse aggregation expression and return column name and aggregation function

        Args:
            source_expr: SQL expression like "sum(sba.subscription_count)"
            df: DataFrame to check column existence
            parent_cte: Parent CTE name (e.g., 'sba_inner')

        Returns:
            Tuple of (column_name, agg_function) or (None, None)
        """

        expr = str(source_expr).lower().strip()

        # Pattern 1: sum(0) -> constant 0
        if expr == 'sum(0)':
            return ('_constant_0', lambda x: 0)

        # Pattern 2: sum(cte.column_name)
        # Example: sum(sba.subscription_count)
        if expr.startswith('sum('):
            # Extract column name
            # Remove 'sum(' and ')'
            inner = expr[4:-1].strip()

            # Remove CTE prefix if present (e.g., "sba.")
            if '.' in inner:
                col_name = inner.split('.')[-1]
            else:
                col_name = inner

            # Check if column exists
            if col_name in df.columns:
                return (col_name, 'sum')
            else:
                # Column doesn't exist, return constant 0
                return ('_constant_0', lambda x: 0)

        # Pattern 3: count(*) or count(column)
        if expr.startswith('count('):
            if 'count(*)' in expr:
                return ('_count_star', 'count')
            else:
                inner = expr[6:-1].strip()
                if '.' in inner:
                    col_name = inner.split('.')[-1]
                else:
                    col_name = inner

                if col_name in df.columns:
                    return (col_name, 'count')
                else:
                    return ('_constant_0', lambda x: 0)

        # Pattern 4: Complex expressions (CASE WHEN)
        if 'case' in expr or 'when' in expr:
            # For now, return constant 0
            # TODO: Parse CASE WHEN expressions
            return ('_constant_0', lambda x: 0)

        # Unknown pattern - return constant 0
        return ('_constant_0', lambda x: 0)

    def _apply_aggregation(
        self,
        df: pd.DataFrame,
        cte_name: str,
        cte_columns: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply aggregation for a CTE

        Args:
            df: Input DataFrame (e.g., sba_inner)
            cte_name: CTE name (e.g., 'sba')
            cte_columns: Column mapping definitions for this CTE

        Returns:
            Aggregated DataFrame
        """

        # Find parent CTE name (e.g., 'sba' -> 'sba_inner')
        parent_cte = self._find_parent_cte(cte_name)

        # IMPORTANT: Before aggregation, create medium/complex dimension columns
        # These columns are defined for this CTE but need to be created from the input df
        transform_cols = cte_columns[
            cte_columns['transformation_type'].isin(['medium', 'complex'])
        ]

        for _, col_info in transform_cols.iterrows():
            col_name = col_info['column_name']
            source_expr = str(col_info['source_expression'])
            transform_type = col_info['transformation_type']

            # Skip if column already exists
            if col_name in df.columns:
                continue

            try:
                if transform_type == 'medium':
                    df[col_name] = self._apply_medium_transformation(df, source_expr, col_name)
                elif transform_type == 'complex':
                    df[col_name] = self._apply_complex_transformation(df, source_expr, col_name)
            except Exception as e:
                print(f"      ⚠️  Failed to create dimension {col_name}: {e}")
                df[col_name] = None

        # Step 1: Identify dimension columns (simple + medium + complex)
        dimension_cols = cte_columns[
            cte_columns['transformation_type'].isin(['simple', 'medium', 'complex'])
        ]['column_name'].tolist()

        # Step 2: Identify aggregation columns
        agg_cols = cte_columns[
            cte_columns['transformation_type'] == 'aggregation'
        ]

        # Step 3: Build aggregation dictionary
        agg_dict = {}
        constant_cols = {}

        for _, col_info in agg_cols.iterrows():
            col_name = col_info['column_name']
            source_expr = col_info['source_expression']

            # Parse aggregation expression
            source_col, agg_func = self._parse_aggregation_expression(source_expr, df, parent_cte)

            if source_col == '_constant_0':
                # This is a constant 0 column
                constant_cols[col_name] = 0
            elif source_col == '_count_star':
                # count(*) - use any column
                if len(df.columns) > 0:
                    agg_dict[col_name] = pd.NamedAgg(column=df.columns[0], aggfunc='count')
            elif source_col is not None and source_col in df.columns:
                agg_dict[col_name] = pd.NamedAgg(column=source_col, aggfunc=agg_func)
            else:
                # Column not found, use constant 0
                constant_cols[col_name] = 0

        # Step 4: Filter dimension columns that exist in df
        existing_dims = [col for col in dimension_cols if col in df.columns]

        if len(existing_dims) == 0:
            print(f"    ⚠️  No dimension columns found for {cte_name}, skipping aggregation")
            return df

        # Step 5: Perform GROUP BY aggregation
        print(f"    Grouping by {len(existing_dims)} dimensions, aggregating {len(agg_dict)} metrics")

        if len(agg_dict) > 0:
            result = df.groupby(existing_dims, as_index=False).agg(**agg_dict)
        else:
            # No aggregations, just get unique combinations of dimensions
            result = df[existing_dims].drop_duplicates().reset_index(drop=True)

        # Add constant columns
        for col_name, value in constant_cols.items():
            result[col_name] = value

        return result

    def validate_simple_columns(
        self,
        cdl_df: pd.DataFrame,
        sample_size: Optional[int] = None
    ) -> List[ValidationResult]:
        """
        Layer 2: Validate simple field mappings

        Args:
            cdl_df: CDL DataFrame to validate
            sample_size: Optional sample size for testing

        Returns:
            List of validation results
        """
        print("\n" + "=" * 80)
        print("LAYER 2: SIMPLE FIELD VALIDATION")
        print("=" * 80)

        # Build final_select CTE
        final_select = self.build_cte('final_select')

        # Get simple columns from mapping
        simple_cols = self.mapping['column_mappings'][
            (self.mapping['column_mappings']['transformation_type'] == 'simple') &
            (self.mapping['column_mappings']['cte_name'] == 'final_select')
        ]

        results = []

        print(f"\nValidating {len(simple_cols)} simple columns...")

        # For now, just validate that columns exist
        for _, col_info in simple_cols.iterrows():
            col_name = col_info['column_name']

            if col_name in cdl_df.columns and col_name in final_select.columns:
                results.append(ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='simple',
                    passed=True,
                    total_rows=len(cdl_df)
                ))
            else:
                results.append(ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='simple',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message=f"Column {col_name} not found"
                ))

        passed = sum(1 for r in results if r.passed)
        print(f"  ✅ Passed: {passed}/{len(results)}")

        return results

    def validate_aggregation_columns(
        self,
        cdl_df: pd.DataFrame,
        sample_size: Optional[int] = None
    ) -> List[ValidationResult]:
        """
        Layer 4: Validate aggregation columns

        Args:
            cdl_df: CDL DataFrame to validate
            sample_size: Optional sample size (not used yet)

        Returns:
            List of validation results
        """
        print("\n" + "=" * 80)
        print("LAYER 4: AGGREGATION VALIDATION")
        print("=" * 80)

        results = []

        # Get aggregation columns from mapping
        agg_cols = self.mapping['column_mappings'][
            (self.mapping['column_mappings']['cte_name'] == 'final_select') &
            (self.mapping['column_mappings']['transformation_type'] == 'aggregation')
        ]

        if len(agg_cols) == 0:
            print("  No aggregation columns found in mapping")
            return results

        # Build final_select CTE from source
        print(f"\nRebuilding final_select CTE from source data...")

        try:
            final_select = self.build_cte('final_select')
            print(f"  ✓ Rebuilt CTE: {len(final_select):,} rows, {len(final_select.columns)} columns")
        except Exception as e:
            print(f"  ❌ Failed to rebuild CTE: {e}")
            import traceback
            traceback.print_exc()

            # Return failure for all columns
            for _, col_info in agg_cols.iterrows():
                results.append(ValidationResult(
                    column_name=col_info['column_name'],
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message=f"Failed to rebuild CTE: {str(e)}"
                ))
            return results

        print(f"  CDL has: {len(cdl_df):,} rows")

        # Get dimension columns for merging
        dim_cols = self.mapping['column_mappings'][
            (self.mapping['column_mappings']['cte_name'] == 'final_select') &
            (self.mapping['column_mappings']['transformation_type'] == 'simple')
        ]['column_name'].tolist()

        # Filter to columns that exist in both DataFrames
        merge_cols = [col for col in dim_cols if col in final_select.columns and col in cdl_df.columns]

        if len(merge_cols) == 0:
            print(f"  ⚠️  No common dimension columns found for comparison")
            for _, col_info in agg_cols.iterrows():
                results.append(ValidationResult(
                    column_name=col_info['column_name'],
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message="No common dimension columns for merge"
                ))
            return results

        print(f"  Using {len(merge_cols)} dimension columns for comparison")

        # Validate each aggregation column
        print(f"\nValidating {len(agg_cols)} aggregation columns...")

        for _, col_info in agg_cols.iterrows():
            col_name = col_info['column_name']

            # Check if column exists in both
            if col_name not in final_select.columns:
                results.append(ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message=f"Column not found in rebuilt CTE"
                ))
                print(f"  {col_name}: ❌ FAIL (not in rebuilt CTE)")
                continue

            if col_name not in cdl_df.columns:
                results.append(ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message=f"Column not found in CDL"
                ))
                print(f"  {col_name}: ❌ FAIL (not in CDL)")
                continue

            # Merge and compare
            try:
                comparison = pd.merge(
                    final_select[merge_cols + [col_name]].rename(columns={col_name: 'source_value'}),
                    cdl_df[merge_cols + [col_name]].rename(columns={col_name: 'cdl_value'}),
                    on=merge_cols,
                    how='outer',
                    indicator=True
                )

                # Fill NaN with 0 for comparison
                comparison['source_value'] = comparison['source_value'].fillna(0)
                comparison['cdl_value'] = comparison['cdl_value'].fillna(0)

                # Calculate delta
                comparison['delta'] = abs(comparison['source_value'] - comparison['cdl_value'])

                max_delta = comparison['delta'].max()
                mismatches = (comparison['delta'] > self.tolerance).sum()

                # Check for rows only in one side
                only_in_source = (comparison['_merge'] == 'left_only').sum()
                only_in_cdl = (comparison['_merge'] == 'right_only').sum()

                result = ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=(mismatches == 0 and only_in_source == 0 and only_in_cdl == 0),
                    total_rows=len(comparison),
                    mismatched_rows=mismatches,
                    max_delta=max_delta
                )

                results.append(result)

                # Print status
                status = "✅ PASS" if result.passed else "❌ FAIL"
                print(f"  {col_name}: {status}")
                if not result.passed:
                    if max_delta > 0:
                        print(f"    Max delta: {max_delta:.2f}")
                        print(f"    Mismatched rows: {mismatches}/{len(comparison)}")
                    if only_in_source > 0:
                        print(f"    Rows only in source: {only_in_source}")
                    if only_in_cdl > 0:
                        print(f"    Rows only in CDL: {only_in_cdl}")

            except Exception as e:
                results.append(ValidationResult(
                    column_name=col_name,
                    cte_name='final_select',
                    transformation_type='aggregation',
                    passed=False,
                    total_rows=len(cdl_df),
                    error_message=f"Comparison error: {str(e)}"
                ))
                print(f"  {col_name}: ❌ FAIL (error: {e})")

        return results

    def validate_derived_metrics(self, cdl_df: pd.DataFrame) -> List[ValidationResult]:
        """
        Layer 5: Validate derived metrics
        (Same as in mapping_based_validator.py)

        Args:
            cdl_df: CDL DataFrame to validate

        Returns:
            List of validation results
        """
        print("\n" + "=" * 80)
        print("LAYER 5: DERIVED METRICS VALIDATION")
        print("=" * 80)

        results = []

        # Get derived metrics from mapping - only sum_of_sums type for now
        derived_metrics = self.mapping['derived_metrics']
        derived_metrics = derived_metrics[derived_metrics['metric_type'] == 'sum_of_sums']

        print(f"Validating {len(derived_metrics)} sum_of_sums metrics...")

        for _, metric in derived_metrics.iterrows():
            result = self._validate_derived_metric(cdl_df, metric)
            results.append(result)

            status = "✅ PASS" if result.passed else "❌ FAIL"
            print(f"  {metric['metric_name']}: {status}")
            if not result.passed:
                print(f"    Max delta: {result.max_delta:.6f}")
                print(f"    Mismatched rows: {result.mismatched_rows}/{result.total_rows}")

        return results

    def _validate_derived_metric(
        self,
        cdl_df: pd.DataFrame,
        metric: pd.Series
    ) -> ValidationResult:
        """Validate a single derived metric"""

        metric_name = metric['metric_name']

        # Hardcoded validations for known metrics
        # TODO: Parse metric_expression and calculate dynamically

        if metric_name == 'TotalAcquisition':
            calculated = (
                cdl_df['acquisition_count'] +
                cdl_df['free_to_paid_conversion_count']
            )
            actual = cdl_df['TotalAcquisition']

        elif metric_name == 'NetAcquisition':
            calculated = (
                cdl_df['acquisition_count'] +
                cdl_df['free_to_paid_conversion_count'] -
                cdl_df['switch_acquisition_count'] -
                cdl_df['reactivation_30day_acquisition_count']
            )
            actual = cdl_df['NetAcquisition']

        elif metric_name == 'NetCancellation':
            calculated = (
                cdl_df['cancellation_count'] -
                cdl_df['switch_cancellation_count'] -
                cdl_df['reactivation_30day_acquisition_count']
            )
            actual = cdl_df['NetCancellation']

        else:
            return ValidationResult(
                column_name=metric_name,
                cte_name='derived',
                transformation_type='derived_metric',
                passed=False,
                total_rows=len(cdl_df),
                error_message=f"Metric {metric_name} not implemented"
            )

        # Compare
        delta = abs(calculated - actual)
        max_delta = delta.max()
        mismatches = (delta > self.tolerance).sum()

        return ValidationResult(
            column_name=metric_name,
            cte_name='derived',
            transformation_type='derived_metric',
            passed=(mismatches == 0),
            total_rows=len(cdl_df),
            mismatched_rows=mismatches,
            max_delta=max_delta
        )

    def generate_report(
        self,
        results: List[ValidationResult],
        output_file: Optional[str] = None
    ) -> Dict:
        """Generate validation summary report"""

        summary = {
            'total': len(results),
            'passed': sum(1 for r in results if r.passed),
            'failed': sum(1 for r in results if not r.passed),
            'by_type': {}
        }

        # Group by transformation type
        for result in results:
            t = result.transformation_type
            if t not in summary['by_type']:
                summary['by_type'][t] = {'passed': 0, 'failed': 0}

            if result.passed:
                summary['by_type'][t]['passed'] += 1
            else:
                summary['by_type'][t]['failed'] += 1

        # Print summary
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        print(f"\nTotal validations: {summary['total']}")
        print(f"  ✅ Passed: {summary['passed']}")
        print(f"  ❌ Failed: {summary['failed']}")
        print(f"  Pass rate: {100.0 * summary['passed'] / summary['total']:.1f}%")

        print(f"\nBy transformation type:")
        for t, counts in summary['by_type'].items():
            total = counts['passed'] + counts['failed']
            print(f"  {t}: {counts['passed']}/{total} passed")

        return summary
