"""
Streamlit console for viewing Data Vault tables.

Run with: streamlit run console.py
"""

import streamlit as st
from db import get_session, get_tables, execute_query_df


# Page config
st.set_page_config(
    page_title="Weather Data Vault Console",
    page_icon="🌤️",
    layout="wide"
)

st.title("🌤️ Weather Data Vault Console")

# Get all tables
try:
    all_tables = get_tables()
except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure to run `python -m db.session` first to initialize the database.")
    st.stop()

# Organize tables by type
hubs = [t for t in all_tables if t.startswith('hub_')]
links = [t for t in all_tables if t.startswith('link_')]
sats = [t for t in all_tables if t.startswith('sat_')]

# Display stats
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Tables", len(all_tables))
with col2:
    st.metric("Hubs", len(hubs))
with col3:
    st.metric("Links", len(links))
with col4:
    st.metric("Satellites", len(sats))

st.divider()


def display_table(table_name: str):
    """Display a table with row count and data."""
    try:
        # Get row count
        with get_session() as con:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Display header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"📊 {table_name}")
        with col2:
            st.metric("Rows", count)

        # Get data
        if count > 0:
            df = execute_query_df(f"SELECT * FROM {table_name}")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No data in this table yet.")

        st.divider()

    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")


# Add refresh button
if st.button("🔄 Refresh Data", type="primary"):
    st.rerun()

# Display Hubs
if hubs:
    st.header("🎯 Hubs (Business Keys)")
    st.caption("Core business entities with immutable keys")

    for table in sorted(hubs):
        display_table(table)
else:
    st.warning("No hub tables found.")

# Display Links
if links:
    st.header("🔗 Links (Relationships)")
    st.caption("Relationships between hubs")

    for table in sorted(links):
        display_table(table)
else:
    st.info("No link tables found.")

# Display Satellites
if sats:
    st.header("🛰️ Satellites (Descriptive Data)")
    st.caption("Time-variant descriptive attributes and facts")

    for table in sorted(sats):
        display_table(table)
else:
    st.info("No satellite tables found.")

# Sidebar with info
with st.sidebar:
    st.header("About")
    st.markdown("""
    This console displays the Data Vault 2.0 schema for weather data.

    **Data Vault Components:**
    - **Hubs**: Core business entities (Resorts, Zones, Stations, Offices)
    - **Links**: Relationships between hubs
    - **Satellites**: Descriptive data and time-series facts

    **Database:** DuckDB
    """)

    st.divider()

    st.header("Quick Actions")

    if st.button("📋 Show Schema"):
        st.code("""
# View schema
python -m db.session

# Drop all tables
from db import drop_all_tables
drop_all_tables()

# Reinitialize
from db import init_db
init_db()
        """, language="python")

    st.divider()

    st.caption("Built with Streamlit + DuckDB")
