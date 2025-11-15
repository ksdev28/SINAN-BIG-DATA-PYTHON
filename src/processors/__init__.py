"""
Processadores de Dados SINAN
"""

from .sinan_data_processor_comprehensive import SINANDataProcessorComprehensive
from .sinan_data_processor_duckdb import SINANDataProcessorDuckDB

__all__ = ['SINANDataProcessorComprehensive', 'SINANDataProcessorDuckDB']

