"""
Validate CSV files before import.
Checks required columns exist and reports row count.
"""

import csv
import os

REQUIRED_COLUMNS = {
    'clients': ['company_name', 'status', 'last_deal_date'],
    'contacts': ['first_name', 'last_name', 'company'],
    'relationships': ['person_a_name', 'person_b_name', 'relationship_type', 'strength'],
    'linkedin': ['First Name', 'Last Name', 'Company'],
    'buildings': ['company_name', 'building_address', 'square_feet'],
}


def validate_csv(filepath, csv_type):
    """
    Validate CSV has required columns for the given type.

    Args:
        filepath: Path to CSV file
        csv_type: One of 'clients', 'contacts', 'relationships', 'linkedin', 'buildings'

    Returns:
        (bool, str) — (is_valid, message)
    """
    if not os.path.exists(filepath):
        return False, f"File not found: {filepath}"

    required = REQUIRED_COLUMNS.get(csv_type)
    if required is None:
        return False, f"Unknown CSV type: {csv_type}. Valid types: {list(REQUIRED_COLUMNS.keys())}"

    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            if not headers:
                return False, "File appears empty or has no headers."

            missing = [col for col in required if col not in headers]

            if missing:
                return False, f"Missing columns: {missing}. Found: {headers}"

            rows = list(reader)
            empty_rows = sum(1 for r in rows if all(not v.strip() for v in r.values() if v))

            msg = f"Valid. {len(rows)} rows found."
            if empty_rows:
                msg += f" ({empty_rows} appear blank.)"
            return True, msg

    except Exception as e:
        return False, f"Error reading file: {e}"


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3:
        valid, msg = validate_csv(sys.argv[1], sys.argv[2])
        status = "PASS" if valid else "FAIL"
        print(f"[{status}] {msg}")
    else:
        print(f"Usage: python validate_csv.py <filepath> <type>")
        print(f"Types: {list(REQUIRED_COLUMNS.keys())}")
