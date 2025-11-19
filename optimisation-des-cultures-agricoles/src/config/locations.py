# src/config/locations.py
from dataclasses import dataclass
from pathlib import Path
import yaml

CONFIG_PATH = Path("config/locations.yml")

@dataclass
class Location:
    name: str
    lat: float
    lon: float
    crop_type: str | None = None
    metadata: dict | None = None

class LocationConfig:
    def __init__(self, config_path: Path = CONFIG_PATH):
        self.config_path = config_path
        self.locations = self._load()

    def _load(self):
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        locations = {}
        for loc in data.get("locations", []):
            l = Location(
                name=loc["name"],
                lat=float(loc["lat"]),
                lon=float(loc["lon"]),
                crop_type=loc.get("crop_type"),
                metadata=loc.get("metadata", {})
            )
            locations[l.name] = l
        return locations

    def get(self, name: str) -> Location:
        if name not in self.locations:
            raise KeyError(f"Location '{name}' not found inside locations.yml")
        return self.locations[name]

    def all(self):
        return list(self.locations.values())
