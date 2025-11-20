# src/config/locations.py
from dataclasses import dataclass
import yaml
from pathlib import Path

@dataclass
class Location:
    name: str
    lat: float
    lon: float

class LocationConfig:
    def __init__(self, path="config/locations.yml"):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # نخزن locations فال dictionary باش نخدمو بالاسم
        self.locations = {
            item["name"]: Location(
                name=item["name"],
                lat=item["lat"],
                lon=item["lon"]
            )
            for item in data.get("locations", [])
        }

    def all(self):
        """Return list of all Location objects."""
        return list(self.locations.values())

    def get(self, name: str):
        """Get one location by its name."""
        if name not in self.locations:
            raise KeyError(f"Location '{name}' not found in locations.yml")
        return self.locations[name]
