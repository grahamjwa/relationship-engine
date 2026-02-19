"""
Reports — Generate and preview weekly reports.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path
from reports.weekly_report import generate_weekly_report, format_report_markdown

st.set_page_config(page_title="Reports", page_icon="📊", layout="wide")

st.title("Reports")

# Controls
c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    report_type = st.selectbox("Report Type", ["Weekly"], key="rtype")
with c2:
    week_ending = st.date_input("Week Ending",
                                value=datetime.now().date(),
                                key="week_end")
with c3:
    pass

if st.button("Generate Report", key="gen_report"):
    with st.spinner("Generating..."):
        report_data = generate_weekly_report(week_ending.strftime('%Y-%m-%d'))
        md = format_report_markdown(report_data)

        st.session_state['last_report'] = md
        st.session_state['last_report_data'] = report_data

if st.session_state.get('last_report'):
    md = st.session_state['last_report']

    st.markdown("---")
    st.markdown(md)

    st.markdown("---")

    # Download
    st.download_button(
        "📥 Download Markdown",
        data=md,
        file_name=f"weekly_report_{st.session_state.get('last_report_data', {}).get('week_ending', 'report')}.md",
        mime="text/markdown"
    )

    # Summary stats
    report_data = st.session_state.get('last_report_data', {})
    with st.sidebar:
        st.subheader("Report Summary")
        st.metric("Meetings", len(report_data.get('meetings', [])))
        st.metric("Deals Moved", len(report_data.get('deals_moved', [])))
        st.metric("Follow-ups Done", len(report_data.get('followups_completed', [])))
        st.metric("Funding Signals", len(report_data.get('funding_signals', [])))
        st.metric("Exec Changes", len(report_data.get('exec_changes', [])))
        st.metric("Overdue Items", len(report_data.get('overdue', [])))
else:
    st.info("Click 'Generate Report' to create a weekly summary.")
