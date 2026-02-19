"""
Data Import Page — Upload and validate CSVs, then import into the engine.
"""

import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path
from importers.validate_csv import validate_csv, REQUIRED_COLUMNS
from import_all import IMPORT_DIR

st.set_page_config(page_title="Import Data", page_icon="📥", layout="wide")

os.makedirs(IMPORT_DIR, exist_ok=True)

st.title("📥 Import Data")
st.caption("Upload CSV files to import contacts, relationships, client data, and buildings.")

# Show expected column templates
with st.expander("CSV Column Requirements"):
    for csv_type, cols in REQUIRED_COLUMNS.items():
        st.markdown(f"**{csv_type}:** {', '.join(cols)}")

st.markdown("---")

# Upload sections
uploads = {}

st.subheader("LinkedIn Connections")
linkedin_file = st.file_uploader("Upload LinkedIn Connections CSV", type=['csv'], key='linkedin')
if linkedin_file:
    dest = os.path.join(IMPORT_DIR, 'linkedin_connections.csv')
    with open(dest, 'wb') as f:
        f.write(linkedin_file.getvalue())
    valid, msg = validate_csv(dest, 'linkedin')
    if valid:
        st.success(f"LinkedIn: {msg}")
        uploads['linkedin'] = dest
    else:
        st.error(f"LinkedIn: {msg}")

st.subheader("Contacts")
contacts_file = st.file_uploader("Upload Contacts CSV", type=['csv'], key='contacts')
if contacts_file:
    dest = os.path.join(IMPORT_DIR, 'contacts.csv')
    with open(dest, 'wb') as f:
        f.write(contacts_file.getvalue())
    valid, msg = validate_csv(dest, 'contacts')
    if valid:
        st.success(f"Contacts: {msg}")
        uploads['contacts'] = dest
    else:
        st.error(f"Contacts: {msg}")

st.subheader("Relationships")
rels_file = st.file_uploader("Upload Relationships CSV", type=['csv'], key='rels')
if rels_file:
    dest = os.path.join(IMPORT_DIR, 'relationships.csv')
    with open(dest, 'wb') as f:
        f.write(rels_file.getvalue())
    valid, msg = validate_csv(dest, 'relationships')
    if valid:
        st.success(f"Relationships: {msg}")
        uploads['relationships'] = dest
    else:
        st.error(f"Relationships: {msg}")

st.subheader("Clients")
clients_file = st.file_uploader("Upload Clients CSV", type=['csv'], key='clients')
if clients_file:
    dest = os.path.join(IMPORT_DIR, 'clients.csv')
    with open(dest, 'wb') as f:
        f.write(clients_file.getvalue())
    valid, msg = validate_csv(dest, 'clients')
    if valid:
        st.success(f"Clients: {msg}")
        uploads['clients'] = dest
    else:
        st.error(f"Clients: {msg}")

st.subheader("Buildings / Leases")
buildings_file = st.file_uploader("Upload Buildings CSV", type=['csv'], key='buildings')
if buildings_file:
    dest = os.path.join(IMPORT_DIR, 'buildings.csv')
    with open(dest, 'wb') as f:
        f.write(buildings_file.getvalue())
    valid, msg = validate_csv(dest, 'buildings')
    if valid:
        st.success(f"Buildings: {msg}")
        uploads['buildings'] = dest
    else:
        st.error(f"Buildings: {msg}")

# Run import
st.divider()

if st.button("Run All Imports", type="primary"):
    with st.spinner("Importing..."):
        from import_all import run_all_imports
        results = run_all_imports()

    if results:
        st.json(results)
        st.success("Import complete. Scores recomputed.")
    else:
        st.warning("No CSV files found in imports directory.")

# Current DB stats
st.divider()
st.subheader("Current Database Stats")

conn = sqlite3.connect(get_db_path())
cur = conn.cursor()
tables = ['companies', 'contacts', 'relationships', 'funding_events',
          'hiring_signals', 'outreach_log', 'buildings', 'leases']
cols = st.columns(4)
for i, t in enumerate(tables):
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        count = cur.fetchone()[0]
        cols[i % 4].metric(t.replace('_', ' ').title(), count)
    except Exception:
        cols[i % 4].metric(t.replace('_', ' ').title(), "N/A")
conn.close()
