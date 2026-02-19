"""
LinkedIn Import — wrapper around root-level linkedin_import.py
"""
import os
import sys

# Add project root to path so we can import the root-level module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from linkedin_import import (  # noqa: E402
    parse_linkedin_csv,
    import_connections,
)

__all__ = ['parse_linkedin_csv', 'import_connections']
