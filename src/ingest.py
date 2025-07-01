import os
import json
import requests
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
URL = "https://api.crossref.org/works"
PARAMS = {"sort": "published", "order": "desc", "rows": 200}

def fetch_crossref():
    resp = requests.get(URL, params=PARAMS, timeout=10)
    resp.raise_for_status()
    return resp.json()["message"]["items"]

def save_raw(items):
    os.makedirs(RAW_DIR, exist_ok=True)
    for idx, item in enumerate(items, start=1):
        doi = item.get("DOI", f"no-doi-{idx}")
        fname = doi.replace("/", "_") + ".json"
        path = os.path.join(RAW_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(item, f, ensure_ascii=False, indent=2)
    print(f"[{datetime.now()}] Saved {len(items)} JSON files to {RAW_DIR}")

items = fetch_crossref()
save_raw(items)
