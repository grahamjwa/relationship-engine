"""Geo Qualification - stub for geographic filtering"""

def is_geo_qualified(company_id, conn=None):
    """Return True by default (no geo filtering)."""
    return True

def get_msa_name(company_id, conn=None):
    """Return default MSA."""
    return "New York"
