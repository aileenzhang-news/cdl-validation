"""
BigQuery Data Loader for CDL Validation

Loads source data directly from BigQuery instead of CSV files.
Executes the same queries used for CSV exports but returns DataFrames directly.
"""

from typing import Dict, Optional
import pandas as pd
import os
from google.cloud import bigquery
from google.auth import default
from pathlib import Path
from dotenv import load_dotenv


class BigQueryDataLoader:
    """Load CDL source data from BigQuery"""

    def __init__(
        self,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        start_date: str = '2000-03-30',
        end_date: str = '2026-03-16'
    ):
        """
        Initialize BigQuery data loader

        Args:
            project_id: GCP project ID (defaults to GOOGLE_PROJECT_ID from .env)
            credentials_path: Optional path to service account JSON key
            start_date: Start date for data filtering (YYYY-MM-DD)
            end_date: End date for data filtering (YYYY-MM-DD)
        """
        # Load .env file if it exists
        load_dotenv()

        # Get project ID from parameter or environment
        self.project_id = project_id or os.getenv('GOOGLE_PROJECT_ID', 'ncau-data-newsquery-prd')
        self.start_date = start_date
        self.end_date = end_date

        # Initialize BigQuery client
        if credentials_path:
            # Option 1: Use service account JSON key file
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
        elif os.getenv('GOOGLE_CLIENT_EMAIL') and os.getenv('GOOGLE_PRIVATE_KEY'):
            # Option 2: Use credentials from .env file
            from google.oauth2 import service_account
            import json

            credentials_info = {
                "type": "service_account",
                "project_id": self.project_id,
                "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
                "private_key": os.getenv('GOOGLE_PRIVATE_KEY'),
                "token_uri": "https://oauth2.googleapis.com/token",
            }

            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
        else:
            # Option 3: Use Application Default Credentials (ADC)
            self.client = bigquery.Client(project=self.project_id)

    def load_all_source_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load all source data tables from BigQuery

        Returns:
            Dictionary with keys: 'sbf', 'smf', 'cal', 'schemas'
        """
        print(f"\nLoading source data from BigQuery...")
        print(f"  Project: {self.project_id}")
        print(f"  Date range: {self.start_date} to {self.end_date}")

        source_data = {}

        # Load subscription base
        print("\n  Loading subscription base...")
        source_data['sbf'] = self._load_subscription_base()
        print(f"  ✓ Subscription Base: {len(source_data['sbf']):,} rows")

        # Load subscription movement
        print("  Loading subscription movement...")
        source_data['smf'] = self._load_subscription_movement()
        print(f"  ✓ Subscription Movement: {len(source_data['smf']):,} rows")

        # Load calendar
        print("  Loading calendar...")
        source_data['cal'] = self._load_calendar()
        print(f"  ✓ Calendar: {len(source_data['cal']):,} rows")

        # Load schemas
        print("  Loading schemas...")
        source_data['schemas'] = self._load_schemas()
        print(f"  ✓ Schemas: {len(source_data['schemas']):,} column definitions")

        return source_data

    def _load_subscription_base(self) -> pd.DataFrame:
        """Load subscription_base_extended_fct from BigQuery"""

        query = f"""
        SELECT
          -- All 28 columns referenced in mapping.xlsx from sbf table
          classification_level_1,
          classification_level_2,
          delivery_medium_type,
          delivery_schedule_days,
          delivery_schedule_group,
          dw_billing_system_code,
          dw_effective_end_datetime,
          dw_effective_start_datetime,
          dw_publication_id,
          dw_rate_plan_id,
          dw_source_system_code,
          frontbook_backbook_group_name,
          hyper_local_brand_name,
          hyper_local_publication_name,
          is_paying_flag,
          local_brand_name,
          local_publication_name,
          masthead,
          offer_category_group_name,
          offer_category_name,
          ratecard_price,
          sold_in_channel,
          sold_in_source,
          sold_in_source_channel,
          sold_in_source_code,
          subscriber_has_email_flag,
          subscription_count,
          subscription_tenure_start_datetime
        FROM `{self.project_id}.bdm_subscription.subscription_base_extended_fct`
        WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '{self.start_date}' AND '{self.end_date}'
           OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '{self.start_date}' AND '{self.end_date}'
        """

        df = self.client.query(query).to_dataframe()

        # Convert datetime columns
        for col in ['dw_effective_start_datetime', 'dw_effective_end_datetime', 'subscription_tenure_start_datetime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    def _load_subscription_movement(self) -> pd.DataFrame:
        """Load subscription_movement_extended_fct from BigQuery"""

        query = f"""
        SELECT
          -- All 34 columns referenced in mapping.xlsx from smf table
          classification_level_1,
          classification_level_2,
          delivery_medium_type,
          delivery_schedule_days,
          delivery_schedule_group,
          dw_billing_system_code,
          dw_publication_id,
          dw_rate_plan_id,
          dw_source_system_code,
          frontbook_backbook_group_name,
          fy_month_of_year,
          fy_week_of_year,
          fy_year,
          hyper_local_brand_name,
          hyper_local_publication_name,
          incremental_subscription_movement_count,
          is_paying_flag,
          local_brand_name,
          local_publication_name,
          masthead,
          movement_datetime,
          movement_type_code,
          offer_category_group_name,
          offer_category_name,
          report_date,
          sold_in_channel,
          sold_in_source,
          sold_in_source_channel,
          sold_in_source_code,
          subscriber_has_email_flag,
          subscription_movement_count,
          subscription_movement_count_sub_type,
          subscription_movement_count_type,
          subscription_tenure_start_datetime
        FROM `{self.project_id}.bdm_subscription.subscription_movement_extended_fct`
        WHERE report_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """

        df = self.client.query(query).to_dataframe()

        # Convert datetime columns
        for col in ['report_date', 'movement_datetime', 'subscription_tenure_start_datetime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    def _load_calendar(self) -> pd.DataFrame:
        """Load v_calendar_dim from BigQuery"""

        query = f"""
        SELECT
          -- All 5 columns referenced in mapping.xlsx from cal table
          fy_month_of_year,
          fy_week_of_year,
          fy_year,
          last_day_of_week AS last_day_of_week_date,
          calendar_date AS report_date
        FROM `{self.project_id}.prstn_consumer.v_calendar_dim`
        WHERE calendar_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """

        df = self.client.query(query).to_dataframe()

        # Convert report_date to datetime
        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'])

        return df

    def _load_schemas(self) -> pd.DataFrame:
        """Load table schemas from BigQuery INFORMATION_SCHEMA"""

        query = f"""
        SELECT
          table_name,
          column_name,
          data_type,
          is_nullable
        FROM `{self.project_id}.bdm_subscription.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name IN ('subscription_base_extended_fct', 'subscription_movement_extended_fct')

        UNION ALL

        SELECT
          table_name,
          column_name,
          data_type,
          is_nullable
        FROM `{self.project_id}.prstn_consumer.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'v_calendar_dim'
        """

        df = self.client.query(query).to_dataframe()
        return df

    def check_data_volume(self) -> pd.DataFrame:
        """
        Check row counts and date ranges for source tables

        Returns:
            DataFrame with table_name, row_count, min_date, max_date
        """
        query = f"""
        -- Check row counts for subscription_base_extended_fct
        SELECT
          'subscription_base_extended_fct' as table_name,
          COUNT(*) as row_count,
          MIN(CAST(dw_effective_start_datetime AS DATE)) as min_date,
          MAX(CAST(dw_effective_start_datetime AS DATE)) as max_date
        FROM `{self.project_id}.bdm_subscription.subscription_base_extended_fct`
        WHERE CAST(dw_effective_start_datetime AS DATE) BETWEEN '{self.start_date}' AND '{self.end_date}'
           OR CAST(dw_effective_end_datetime AS DATE) BETWEEN '{self.start_date}' AND '{self.end_date}'

        UNION ALL

        -- Check row counts for subscription_movement_extended_fct
        SELECT
          'subscription_movement_extended_fct' as table_name,
          COUNT(*) as row_count,
          MIN(CAST(report_date AS DATE)) as min_date,
          MAX(CAST(report_date AS DATE)) as max_date
        FROM `{self.project_id}.bdm_subscription.subscription_movement_extended_fct`
        WHERE CAST(report_date AS DATE) BETWEEN '{self.start_date}' AND '{self.end_date}'

        UNION ALL

        -- Check row counts for v_calendar_dim
        SELECT
          'v_calendar_dim' as table_name,
          COUNT(*) as row_count,
          MIN(calendar_date) as min_date,
          MAX(calendar_date) as max_date
        FROM `{self.project_id}.prstn_consumer.v_calendar_dim`
        WHERE calendar_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """

        df = self.client.query(query).to_dataframe()
        return df
