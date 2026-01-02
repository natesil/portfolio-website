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

# Get all tables across all schemas
try:
    with get_session() as con:
        result = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('main', 'bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
        """).fetchall()

        # Create fully qualified table names
        all_tables = []
        table_map = {}
        for schema, table in result:
            if schema == 'main':
                full_name = table
            else:
                full_name = f"{schema}.{table}"
            all_tables.append(full_name)
            table_map[full_name] = (schema, table)

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure to run `dbt run` first to create the tables.")
    st.stop()

# Organize tables by type (check base table name, not schema)
bronze = [t for t in all_tables if 'bronze_' in t]
hubs = [t for t in all_tables if 'hub_' in t]
links = [t for t in all_tables if 'link_' in t]
sats = [t for t in all_tables if 'sat_' in t]

# Display stats
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Tables", len(all_tables))
with col2:
    st.metric("Bronze", len(bronze))
with col3:
    st.metric("Hubs", len(hubs))
with col4:
    st.metric("Links", len(links))
with col5:
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

# Display Bronze Layer
if bronze:
    st.header("🥉 Bronze Layer (Flattened Raw Data)")
    st.caption("Flattened tables loaded from JSON files in datalake/raw/")

    for table in sorted(bronze):
        display_table(table)
else:
    st.warning("No bronze tables found. Run `dbt run --select bronze.*` to create bronze tables.")

# Display Hubs
if hubs:
    st.header("🎯 Hubs (Business Keys)")
    st.caption("Core business entities with immutable keys")

    for table in sorted(hubs):
        display_table(table)
else:
    st.warning("No hub tables found. Run `dbt run --select silver.hubs.*` to create hubs.")

# Display Links
if links:
    st.header("🔗 Links (Relationships)")
    st.caption("Relationships between hubs")

    for table in sorted(links):
        display_table(table)
else:
    st.info("No link tables found. Run `dbt run --select silver.links.*` to create links.")

# Display Satellites
if sats:
    st.header("🛰️ Satellites (Descriptive Data)")
    st.caption("Time-variant descriptive attributes and facts")

    for table in sorted(sats):
        display_table(table)
else:
    st.info("No satellite tables found. Run `dbt run --select silver.satellites.*` to create satellites.")

# Sidebar with info
with st.sidebar:
    st.header("About")
    st.markdown("""
    This console displays the Bronze and Silver layers for weather data.

    **Bronze Layer (Flattened Raw):**
    - Raw data loaded from JSON files
    - Flattened structures for easy querying

    **Silver Layer (Data Vault 2.0):**
    - **Hubs**: Core business entities (Resorts, Zones, Stations, Offices)
    - **Links**: Relationships between hubs
    - **Satellites**: Descriptive data and time-series facts

    **Database:** DuckDB
    """)

    st.divider()

    st.header("Quick Actions")

    if st.button("📋 dbt Commands"):
        st.code("""
# Run all models
cd db/data_model && dbt run

# Run specific layer
dbt run --select bronze.*
dbt run --select silver.*

# Full refresh (rebuild)
dbt run --full-refresh

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
        """, language="bash")

    st.divider()

    st.caption("Built with Streamlit + DuckDB")
