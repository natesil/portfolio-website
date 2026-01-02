"""
Configuration loader for ski resort data pipeline.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any
from pydantic import BaseModel


class Location(BaseModel):
    """Geographic location."""
    latitude: float
    longitude: float


class ResortMetadata(BaseModel):
    """Additional resort metadata."""
    full_name: str
    region: str


class ResortConfig(BaseModel):
    """Single resort configuration."""
    name: str
    state: str
    location: Location
    metadata: ResortMetadata


class ResortsConfig(BaseModel):
    """Complete resorts configuration."""
    resorts: List[ResortConfig]


def load_resorts_config(config_path: str = "config/resorts.yaml") -> ResortsConfig:
    """
    Load resort configuration from YAML file.

    Args:
        config_path: Path to resorts.yaml file

    Returns:
        ResortsConfig with validated data
    """
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, 'r') as f:
        data = yaml.safe_load(f)

    return ResortsConfig(**data)


def get_resort_by_name(config: ResortsConfig, name: str) -> ResortConfig:
    """Get a specific resort by name."""
    for resort in config.resorts:
        if resort.name.lower() == name.lower():
            return resort
    raise ValueError(f"Resort not found: {name}")


if __name__ == "__main__":
    # Example usage
    config = load_resorts_config()

    print(f"Loaded {len(config.resorts)} resorts:\n")

    for resort in config.resorts:
        print(f"{resort.name}, {resort.state}")
        print(f"  Location: {resort.location.latitude}, {resort.location.longitude}")
        print(f"  Region: {resort.metadata.region}")
        print()
