"""
Test script to verify backend setup.

Run this to ensure all components are working correctly.
"""

import os
from pathlib import Path


def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")

    try:
        from models.api import PointsResponse, GridForecastResponse
        from clients import WeatherClient
        from config import load_resorts_config
        from db import get_connection, get_session, init_db, generate_hash_key
        print("  ✓ All imports successful")
        return True
    except Exception as e:
        print(f"  ✗ Import failed: {e}")
        return False


def test_config():
    """Test configuration loading."""
    print("\nTesting configuration...")

    try:
        from config import load_resorts_config

        config = load_resorts_config()
        print(f"  ✓ Loaded {len(config.resorts)} resorts:")

        for resort in config.resorts:
            print(f"    - {resort.name}, {resort.state}")
            print(f"      ({resort.location.latitude}, {resort.location.longitude})")

        return True
    except Exception as e:
        print(f"  ✗ Config test failed: {e}")
        return False


def test_database():
    """Test database initialization."""
    print("\nTesting database...")

    try:
        from db import init_db, get_session, get_tables

        # Initialize database
        init_db()

        # Test session
        with get_session() as con:
            result = con.execute("SELECT 1 as test").fetchone()
            print(f"  ✓ Database connection successful (test query returned: {result[0]})")

        # Check tables
        tables = get_tables()
        print(f"  ✓ Created {len(tables)} tables:")

        for table in tables[:5]:  # Show first 5
            print(f"    - {table}")

        if len(tables) > 5:
            print(f"    ... and {len(tables) - 5} more")

        return True
    except Exception as e:
        print(f"  ✗ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_api_client():
    """Test weather API client."""
    print("\nTesting API client...")

    try:
        from clients import WeatherClient
        from config import load_resorts_config

        config = load_resorts_config()
        client = WeatherClient()

        # Test with first resort
        resort = config.resorts[0]
        print(f"  Testing with {resort.name}, {resort.state}...")

        # Get points data
        points = client.get_points(resort.location.latitude, resort.location.longitude)

        print(f"  ✓ Points API call successful")
        print(f"    Grid: {points.properties.gridId} ({points.properties.gridX}, {points.properties.gridY})")
        print(f"    Zone: {points.properties.forecastZone.split('/')[-1]}")

        # Get forecast
        forecast = client.get_forecast_from_points(points)
        print(f"  ✓ Forecast API call successful")
        print(f"    Next period: {forecast.properties.periods[0].name}")
        print(f"    {forecast.properties.periods[0].shortForecast}")

        return True
    except Exception as e:
        print(f"  ✗ API client test failed: {e}")
        print(f"    (This is expected if you don't have internet connection)")
        return False


def test_environment():
    """Test environment setup."""
    print("\nTesting environment...")

    env_file = Path(".env")
    env_example = Path(".env.example")

    if env_example.exists():
        print(f"  ✓ .env.example exists")
    else:
        print(f"  ✗ .env.example not found")

    if env_file.exists():
        print(f"  ✓ .env file exists")
    else:
        print(f"  ⚠ .env file not found (using defaults)")

    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("Backend Setup Verification")
    print("=" * 60)

    results = {
        "Environment": test_environment(),
        "Imports": test_imports(),
        "Configuration": test_config(),
        "Database": test_database(),
        "API Client": test_api_client(),
    }

    print("\n" + "=" * 60)
    print("Test Results:")
    print("=" * 60)

    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name:.<40} {status}")

    all_passed = all(results.values())

    print("=" * 60)

    if all_passed:
        print("\n🎉 All tests passed! Backend is ready to use.")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")

    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
