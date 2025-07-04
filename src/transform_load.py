# src/transform_load.py
import os , sys
import json
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, text

# ───── CONFIG ────────────────────────────────────────────────────────────────
# 1) Point this at your MySQL DW (replace placeholders):
DB_URI = "mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db"
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


raw = load_raw_items(RAW_DIR)
dims = normalize(raw)
create_and_load(*dims)
