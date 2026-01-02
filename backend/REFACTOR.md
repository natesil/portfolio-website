# SQLAlchemy → Raw DuckDB Refactor

## Summary

Removed SQLAlchemy ORM in favor of raw DuckDB SQL for better compatibility and performance with analytical workloads.

## What Changed

### Removed
- ❌ SQLAlchemy dependency
- ❌ duckdb-engine dependency
- ❌ `models/vault.py` (ORM models) → renamed to `vault.py.old` for reference

### Added
- ✅ `db/schema.sql` - Pure SQL DDL for Data Vault schema
- ✅ `db/utils.py` - Hash key generation utility
- ✅ Updated `db/session.py` - Raw DuckDB Python API

### Kept
- ✅ `models/api.py` - Pydantic models for API validation (unchanged)
- ✅ `clients/weather.py` - API client (unchanged)
- ✅ `config.py` - Config loader (unchanged)
- ✅ All project structure

## Benefits

1. **No Compatibility Issues** - Direct DuckDB API, no translation layer
2. **Better Performance** - No ORM overhead for analytical queries
3. **Full SQL Control** - Write optimized analytical SQL
4. **Cleaner Code** - Simpler, more explicit
5. **Portfolio Value** - Shows understanding of when NOT to use an ORM

## New API

### Before (SQLAlchemy)
```python
from models.vault import HubResort, SatForecastPeriod
from db import get_session

with get_session() as session:
    resort = HubResort(resort_key="abc", resort_name="Sugarloaf")
    session.add(resort)
    session.commit()
```

### After (Raw DuckDB)
```python
from db import get_session, generate_hash_key

with get_session() as con:
    resort_key = generate_hash_key("Sugarloaf")
    con.execute("""
        INSERT INTO hub_resort (resort_key, resort_name)
        VALUES (?, ?)
    """, (resort_key, "Sugarloaf"))
```

### Querying Data
```python
from db import execute_query, execute_query_df

# Get results as tuples
results = execute_query("SELECT * FROM hub_resort")

# Get results as pandas DataFrame
df = execute_query_df("SELECT * FROM sat_forecast_period WHERE resort_key = ?", (key,))
```

## Testing

Run the test script to verify everything works:

```bash
python test_setup.py
```

Should see:
- ✓ Imports successful
- ✓ Config loads correctly
- ✓ Database initializes from schema.sql
- ✓ API client works
