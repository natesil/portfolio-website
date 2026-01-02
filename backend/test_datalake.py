"""
Test script to demonstrate config-driven data lake collection.

This shows the full flow:
1. Read resorts from config
2. Fetch data from weather.gov API for each resort
3. Save to data lake (raw JSON)
4. List and verify saved files
"""

from clients import WeatherClient
from datalake import collect_all_resorts, list_raw_files


def main():
    print("=" * 60)
    print("Config-Driven Data Lake Test")
    print("=" * 60)

    print("\nThis will:")
    print("  - Load resorts from config/resorts.yaml")
    print("  - Fetch forecast data for each resort")
    print("  - Save raw JSON to datalake/raw/")

    # Create API client
    print("\nCreating API client...")
    client = WeatherClient()

    # Collect data for all resorts from config
    print("\nCollecting data...\n")

    try:
        results = collect_all_resorts(client)

    except Exception as e:
        print(f"\n✗ Collection failed: {e}")
        print("(This is expected if you don't have internet)")
        return

    # Summary
    total_files = sum(len(files) for files in results.values())
    successful = sum(1 for files in results.values() if files)

    print("\n" + "=" * 60)
    print("✓ Collection Complete!")
    print("=" * 60)

    print(f"\nResorts: {successful}/{len(results)}")
    print(f"Total files: {total_files}")

    # Show what was saved
    print("\nSaved files:")
    for resort_name, files in results.items():
        if files:
            print(f"\n  {resort_name}:")
            for category, filepath in files.items():
                print(f"    - {category}: {filepath.name}")

    # List all files in data lake
    print("\n" + "=" * 60)
    print("Data Lake Contents")
    print("=" * 60)

    for category in ['points', 'forecasts', 'hourly']:
        files = list_raw_files(category)
        if files:
            print(f"\n{category}/: {len(files)} file(s)")
            for f in files[:3]:
                print(f"  - {f.name}")
            if len(files) > 3:
                print(f"  ... and {len(files) - 3} more")

    print("\n" + "=" * 60)
    print("Next Steps")
    print("=" * 60)
    print("\n1. Run collection daily:")
    print("   python -m datalake.writer")
    print("\n2. Build ETL to load Data Vault:")
    print("   python -m flows.etl  (coming soon)")
    print("\n3. View data in console:")
    print("   streamlit run console.py")


if __name__ == "__main__":
    main()
