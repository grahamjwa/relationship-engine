"""
Master import script — imports all CSVs in data/imports/.

Place CSV files in data/imports/ and run this script.
Expected files: linkedin_connections.csv, contacts.csv, relationships.csv,
                clients.csv, buildings.csv
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

from core.graph_engine import get_db_path
from linkedin_import import import_connections as import_linkedin_connections
from import_contacts import import_contacts as import_contacts_csv
from import_relationships import import_relationships as import_relationships_csv
from import_clients import import_clients_csv
from import_buildings import import_buildings_csv

IMPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'imports')


def run_all_imports(db_path=None):
    """Run all available imports from data/imports/."""
    if db_path is None:
        db_path = get_db_path()

    results = {}

    # LinkedIn
    linkedin_path = os.path.join(IMPORT_DIR, 'linkedin_connections.csv')
    if os.path.exists(linkedin_path):
        try:
            r = import_linkedin_connections(linkedin_path, db_path=db_path)
            results['linkedin'] = r
            print(f"LinkedIn: {r}")
        except Exception as e:
            results['linkedin'] = f"Error: {e}"
            print(f"LinkedIn: Error — {e}")

    # Contacts
    contacts_path = os.path.join(IMPORT_DIR, 'contacts.csv')
    if os.path.exists(contacts_path):
        try:
            r = import_contacts_csv(contacts_path, db_path=db_path)
            results['contacts'] = r
            print(f"Contacts: {r}")
        except Exception as e:
            results['contacts'] = f"Error: {e}"
            print(f"Contacts: Error — {e}")

    # Relationships
    rels_path = os.path.join(IMPORT_DIR, 'relationships.csv')
    if os.path.exists(rels_path):
        try:
            r = import_relationships_csv(rels_path, db_path=db_path)
            results['relationships'] = r
            print(f"Relationships: {r}")
        except Exception as e:
            results['relationships'] = f"Error: {e}"
            print(f"Relationships: Error — {e}")

    # Clients
    clients_path = os.path.join(IMPORT_DIR, 'clients.csv')
    if os.path.exists(clients_path):
        try:
            r = import_clients_csv(clients_path, db_path=db_path)
            results['clients'] = r
            print(f"Clients: {r}")
        except Exception as e:
            results['clients'] = f"Error: {e}"
            print(f"Clients: Error — {e}")

    # Buildings
    buildings_path = os.path.join(IMPORT_DIR, 'buildings.csv')
    if os.path.exists(buildings_path):
        try:
            r = import_buildings_csv(buildings_path, db_path=db_path)
            results['buildings'] = r
            print(f"Buildings: {r}")
        except Exception as e:
            results['buildings'] = f"Error: {e}"
            print(f"Buildings: Error — {e}")

    # Recompute scores after import
    if results:
        print("\nRecomputing opportunity scores...")
        try:
            from core.opportunity_scoring import save_opportunity_scores
            save_opportunity_scores(db_path=db_path)
            print("Scores recomputed.")
        except Exception as e:
            print(f"Score recompute skipped: {e}")

    return results


def list_pending_imports():
    """List CSV files waiting in imports directory."""
    if not os.path.exists(IMPORT_DIR):
        return []
    return [f for f in os.listdir(IMPORT_DIR)
            if f.endswith('.csv') and not f.startswith('.')]


if __name__ == "__main__":
    os.makedirs(IMPORT_DIR, exist_ok=True)
    pending = list_pending_imports()
    if pending:
        print(f"Found {len(pending)} CSV files to import: {pending}")
        print("\nRunning imports...")
        results = run_all_imports()
        print(f"\nResults: {results}")
    else:
        print(f"No CSV files found in: {IMPORT_DIR}")
        print("Expected files: linkedin_connections.csv, contacts.csv, "
              "relationships.csv, clients.csv, buildings.csv")
