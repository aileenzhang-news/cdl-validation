"""
CDL Validation Framework - Validators Package

This package contains multiple validation tools for the CDL (Consolidated Data Layer):

1. LocalSourceValidator - Validates CDL by rebuilding transformations from source tables
2. TableComparisonValidator - Compares two BigQuery tables directly
3. MappingBasedValidator - Legacy mapping-based validation
4. BigQueryDataLoader - Loads source data from BigQuery
5. LLMReportGenerator - Generates AI-powered testing reports
"""

from .local_source_validator import LocalSourceValidator
from .table_comparison_validator import TableComparisonValidator
from .bq_data_loader import BigQueryDataLoader

__all__ = [
    'LocalSourceValidator',
    'TableComparisonValidator',
    'BigQueryDataLoader'
]
