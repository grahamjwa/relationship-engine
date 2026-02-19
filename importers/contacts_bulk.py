"""
Contacts Bulk Import — wrapper around root-level import_contacts.py

Also provides a batch_import function for programmatic use.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from import_contacts import (  # noqa: E402
    get_or_create_company,
    import_contacts,
)

__all__ = ['get_or_create_company', 'import_contacts']
