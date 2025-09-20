import json
from pathlib import Path

def load_metadata(path="C:/Users/Acer/Documents/processing-framework/configs/metadata.json"):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Metadata not found at {path}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)
