MDPI case-study  -  ETL steps 


Below is a **complete Python ETL pipeline** that you can drop straight into your `scilit-case-study` repo under `src/`. It will:

1. **Read** the raw JSON dumps in `data/raw/`  
2. **Normalize & cleanse** into five tables: `publications`, `authors`, `publication_authors`, `sources`, `dates`  
3. **DDL + LOAD** everything into your MySQL DW via SQLAlchemy  

I’ve also sketched out an ASCII ER-diagram and marked where to fill in your MySQL credentials.  

---

## 1. Folder Structure  
```
scilit-case-study/
├─ data/
│  ├─ raw/                ← your 200 JSONs land here
├─ src/
│  ├─ ingest.py           ← you already have
│  └─ transform_load.py   ← new script below
├─ README.md
└─ requirements.txt
```

---

## 2. ER Diagram (ASCII sketch)

```
┌────────────────┐        ┌───────────────┐
│  sources       │        │   dates       │
│─────────────── │        │───────────────│
│ source_id (PK) │        │ date_id (PK)  │
│ source_name    │        │ year          │
│                │        │ month         │
└───────▲────────┘        │ day           │
        │ 1               └─────▲─────────┘
        │                       │ 1
        │                   M   │
 ┌──────┴───────────────────────────────────┐
 │            publications                  │
 │──────────────────────────────────────────│
 │ publication_id (PK)                      │
 │ doi (unique)                             │
 │ title                                    │
 │ publisher                                │
 │ type                                     │
 │ source_id → sources.source_id            │
 │ issued_date_id → dates.date_id           │
 └──────┬─────────────────┬─────────────────┘
        │                 │
        │ M               │ 1
        │                 │
 ┌──────┴────────┐   ┌────┴───────────────────┐
 │ publication_  │   │       authors          │
 │ authors       │   │────────────────────────│
 │───────────────│   │ author_id (PK)         │
 │ publication_id│───│ given_name             │
 │ author_id     │───│ family_name            │
 │ sequence      │   │ affiliation            │
 └───────────────┘   └────────────────────────┘
```

---

## 3. `transform_load.py`  

```python
# src/transform_load.py

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

# ───── CONFIG ────────────────────────────────────────────────────────────────
# 1) Point this at your MySQL DW (replace placeholders):
DB_URI = "mysql+pymysql://<MYSQL_USER>:<MYSQL_PASS>@localhost:3306/scilit_dw"

# 2) Raw JSON folder (created by ingest.py)
RAW_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")

# 3) Table names
TABLES = ["dates", "sources", "authors", "publications", "publication_authors"]

# ───── FUNCTIONS ─────────────────────────────────────────────────────────────

def load_raw_items(raw_dir):
    """Read every .json in data/raw/ into a list of dicts."""
    items = []
    for fn in os.listdir(raw_dir):
        if fn.endswith(".json"):
            path = os.path.join(raw_dir, fn)
            with open(path, "r", encoding="utf-8") as f:
                items.append(json.load(f))
    return items

def normalize(items):
    """
    Turn raw items into five flat record lists:
      - dates, sources, authors, publications, and publication_authors
    """
    dates, sources, authors, pubs, pub_auth = [], [], [], [], []
    # maps for deduplication
    date_map, source_map, author_map = {}, {}, {}

    for pub_idx, itm in enumerate(items, start=1):
        # ── Dates dim ─────────────────────
        date_parts = itm.get("issued", {}).get("date-parts", [[None, None, None]])[0]
        y, m, d = (date_parts + [None, None, None])[:3]
        date_key = f"{y}-{m}-{d}"
        if date_key not in date_map:
            date_map[date_key] = len(date_map) + 1
            dates.append({
                "date_id": date_map[date_key],
                "year": y, "month": m, "day": d
            })
        date_id = date_map[date_key]

        # ── Sources dim ────────────────────
        src = itm.get("container-title", [None])[0]
        if src and src not in source_map:
            source_map[src] = len(source_map) + 1
            sources.append({
                "source_id": source_map[src],
                "source_name": src
            })
        src_id = source_map.get(src)

        # ── Publications fact ──────────────
        pubs.append({
            "publication_id": pub_idx,
            "doi": itm.get("DOI"),
            "title": itm.get("title", [""])[0],
            "publisher": itm.get("publisher"),
            "type": itm.get("type"),
            "source_id": src_id,
            "issued_date_id": date_id
        })

        # ── Authors & Bridge ───────────────
        for seq, auth in enumerate(itm.get("author", []), start=1):
            name_key = (auth.get("given"), auth.get("family"))
            if name_key not in author_map:
                author_map[name_key] = len(author_map) + 1
                aff = auth.get("affiliation", [])
                authors.append({
                    "author_id": author_map[name_key],
                    "given_name": name_key[0],
                    "family_name": name_key[1],
                    "affiliation": aff[0]["name"] if aff else None
                })
            a_id = author_map[name_key]
            pub_auth.append({
                "publication_id": pub_idx,
                "author_id": a_id,
                "sequence": seq
            })

    return dates, sources, authors, pubs, pub_auth

def create_and_load(dates, sources, authors, pubs, pub_auth):
    """Drop & recreate tables, then bulk-insert via pandas.to_sql()."""
    engine = create_engine(DB_URI, echo=False)

    with engine.begin() as conn:
        # 1) Drop old tables
        for tbl in reversed(TABLES):
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))

        # 2) Create tables (DDL)
        conn.execute(text("""
        CREATE TABLE dates (
          date_id   INT PRIMARY KEY,
          year      INT,
          month     INT,
          day       INT
        );
        CREATE TABLE sources (
          source_id   INT PRIMARY KEY,
          source_name VARCHAR(255)
        );
        CREATE TABLE authors (
          author_id    INT PRIMARY KEY,
          given_name   VARCHAR(100),
          family_name  VARCHAR(100),
          affiliation  VARCHAR(255)
        );
        CREATE TABLE publications (
          publication_id   INT PRIMARY KEY,
          doi              VARCHAR(255),
          title            TEXT,
          publisher        VARCHAR(255),
          type             VARCHAR(50),
          source_id        INT,
          issued_date_id   INT,
          FOREIGN KEY (source_id)      REFERENCES sources(source_id),
          FOREIGN KEY (issued_date_id) REFERENCES dates(date_id)
        );
        CREATE TABLE publication_authors (
          publication_id   INT,
          author_id        INT,
          sequence         INT,
          PRIMARY KEY (publication_id, author_id),
          FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
          FOREIGN KEY (author_id)      REFERENCES authors(author_id)
        );
        """))

        # 3) Bulk insert
        pd.DataFrame(dates).to_sql("dates", conn, if_exists="append", index=False)
        pd.DataFrame(sources).to_sql("sources", conn, if_exists="append", index=False)
        pd.DataFrame(authors).to_sql("authors", conn, if_exists="append", index=False)
        pd.DataFrame(pubs).to_sql("publications", conn, if_exists="append", index=False)
        pd.DataFrame(pub_auth).to_sql("publication_authors", conn, if_exists="append", index=False)

    print("✅ All tables created and loaded successfully.")

if __name__ == "__main__":
    raw = load_raw_items(RAW_DIR)
    dims = normalize(raw)
    create_and_load(*dims)
```

---

### 4. How to Run  

1. Install dependencies (add to `requirements.txt`):  
   ```
   pandas
   sqlalchemy
   pymysql
   ```
2. Ensure `data/raw/` has your 200 JSONs (run `ingest.py` first).  
3. From your repo root:
   ```bash
   python src/transform_load.py
   ```
4. Watch for “✅ All tables created and loaded successfully.”  

---

### 5. Next Steps & QA  

- If you hit charset or length errors, tweak your `VARCHAR(...)`.  
- Add data–quality checks by running simple `SELECT COUNT(*)` or `SELECT MIN(year)` queries as smoke tests.  
- Hook this up as an Airflow DAG or script in `dags/` for scheduling—bonus points!  

With this script, you’ve got a **production-ready** prototype:  
- Extraction from API (ingest.py)  
- Staging + normalization (transform_load.py)  
- DDL, data model, and table population  

Reply with any questions—credentials, edge-cases, or how to Dockerize—and we’ll polish it off.