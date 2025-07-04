#!/usr/bin/env python
# coding: utf-8

# ## Extract part 

# In[12]:


# extracting all the licences from the given API
url = 'https://api.crossref.org/works?sort=published&order=desc&rows=200' 
base_url = 'https://api.crossref.org/works'
params = {"sort": "published", "order": "desc", "rows": 200}


# In[14]:


response = requests.get(base_url, params=params)
response.raise_for_status()
json_data = response.json()


# In[27]:


# 2. Drill into the items list
items = json_data["message"]["items"] # pitatiyasto bashook na ["message"]["items"]
items


# In[29]:


# 3. Extract license blocks, if present
#    Each item["license"] is itself a list of dicts describing one or more licenses.
all_licenses = [item.get("license", []) for item in items]
all_licenses


# In[30]:


# 4. (Optional) Flatten and pull just the URLs out
license_urls = [lic_block["URL"] for lic_list in all_licenses for lic_block in lic_list]


# In[31]:


license_urls


# ### ETL for the case study

# #### src/ingest.py

# In[2]:


import os
import json
import requests
from datetime import datetime


# In[ ]:


# optional cell 
# adding path on both ways either we work like running pythn liek program in pythonscript.py or in interactive mode (UI) 
import os

try:
    # when running as a .py file
    ROOT = os.path.dirname(__file__)
except NameError:
    # when running in a notebook
    ROOT = os.getcwd()

RAW_DIR = os.path.join(ROOT, "data", "raw")


# In[27]:


os.getcwd()


# In[28]:


RAW_DIR


# In[8]:


RAW_DIR = "data/raw/"
URL = "https://api.crossref.org/works"
PARAMS = {"sort": "published", "order": "desc", "rows": 200}


# In[48]:


# optinal
# RAW_DIR = "data/files"


# In[9]:


def fetch_crossref():
    resp = requests.get(URL, params=PARAMS, timeout=10)
    resp.raise_for_status()
    return resp.json()["message"]["items"]


# In[49]:


def save_raw(items):# # PuNJENJE folder path-a koji definisemo putem RAW_DIR variable 
    os.makedirs(RAW_DIR, exist_ok=True)
    for idx, item in enumerate(items, start=1):
        doi = item.get("DOI", f"no-doi-{idx}")
        # sanitize filename
        fname = doi.replace("/", "_") + ".json"
        path = os.path.join(RAW_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(item, f, ensure_ascii=False, indent=2)
    print(f"[{datetime.now()}] Saved {len(items)} JSON files to {RAW_DIR}")


# In[50]:


items = fetch_crossref() # calling first function 
save_raw(items)  # calling second function on items we created by first function 


# In[12]:


type(items)


# In[19]:


items


# #### src/transform_load.py

# In[3]:


import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import sqlalchemy


# In[ ]:


#OPTIONAL DONT RUN IT! 
# Tell Python where to find src/ At the top of your notebook:
import os, sys

# adjust the path so Python can import from src/
sys.path.insert(0, os.path.abspath("src"))

# now you can import your ETL functions
from transform_load import load_raw_items, normalize, create_and_load


# In[ ]:


# OPTIONAL DONT RUN IT!
# Override RAW-DIR if needed
# after importing, re-point RAW_DIR for notebook context
from transform_load import RAW_DIR as _orig_raw_dir
import os
RAW_DIR = os.path.abspath("data/raw")


# In[ ]:


# OPTIONAL DONT RUN IT!
# Run your pipeline interactively
raw = load_raw_items(RAW_DIR)
dates, sources, authors, pubs, pub_auth = normalize(raw)
create_and_load(dates, sources, authors, pubs, pub_auth)


# ######    ───── CONFIG  part   ───── ───── ───── ───── ───── ───── ───── ─────

# In[25]:


# 1) Point this at your MySQL DW (replace placeholders):
DB_URI = "mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db"


# In[26]:


DB_URI


# In[40]:


# validation script 
# DODATNO UREDJIVANJE VIDETI KAKO DA SE TOKENIZUJU KREDENCIJALI DA NE BUDU "OGOLJENI" U SKRIPTI 

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def validate_mysql_connection(db_user, db_password, db_host, db_name):
    
    connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(connection_string) #None  # Initialize engine outside try block

    try:
        with engine.connect() as conn:
            # Execute a simple query to verify connection
            version = conn.execute(text("SELECT DATABASE();")).scalar()
            print(f"Successfully connected to MySQL. Database version: {version}")
            return True
    except SQLAlchemyError as e:
        print(f"Error connecting to MySQL: {e}")
        return False
    finally:
        if engine:
            engine.dispose() # Dispose of the engine to close connections in the pool
            
if __name__ == "__main__":
# Replace with your actual database credentials
    USER = "Milosh_85"
    PASSWORD = "Nikoljdan2021"
    HOST = "127.0.0.1"
    DATABASE = "scilit_db"

validate_mysql_connection(USER, PASSWORD, HOST, DATABASE)


# In[34]:


validate_mysql_connection(USER, PASSWORD, HOST, DATABASE)


# In[78]:


os.getcwd()


# In[82]:


# 2) Raw JSON folder (created by ingest.py)
RAW_DIR = os.path.join(os.path.dirname(os.getcwd()), "MDPI\data\\raw")


# In[83]:


RAW_DIR


# In[17]:


# Table names

TABLES = ["dates", "sources", "authors", "publications", "publication_authors"]


# In[18]:


TABLES


# ######  ───── FUNCTIONS part ────────────────────────────────────────────────────────────
# 

# In[68]:


def load_raw_items(raw_dir):# imamo praznu listu, zatim kreiramo putanju gde nam je prvi deo apstrakcija/ direktorijum raw_dir 
    """Read every .json in data/raw/ into a list of dicts."""#  na koji nadovezujemo ime objekta unutar direktorijuma
    items = [] # zatim otvaramo context manager sa with open() dajemo mu apsktakciju f , citamo sadrzaj contexta file-like objecta 
    for fn in os.listdir(raw_dir):#koji zatim uvcitavamo u native python object , items tj lista
        if fn.endswith(".json"):
            path = os.path.join(raw_dir, fn)
            with open(path, "r", encoding="utf-8") as f:
                items.append(json.load(f))
    return items


# 01/07/2025 krecem sa def_Normalize() funckijom 

# In[20]:


def normalize(items):
    """
    Turn raw items into five flat record lists:
      - dates, sources, authors, publications, and publication_authors
    """
    dates, sources, authors, pubs, pub_auth = [], [], [], [], []  #5 empty lists to store final rows for each table
    date_map, source_map, author_map = {}, {}, {} # 3 empty dictionaries for deduplication (so we don’t re-insert the same source, date, or author)

    for pub_idx, itm in enumerate(items, start=1):  # 🔁 Loop through every publication itm, and keep count (pub_idx) starting at 1 
        # ── Dates dim ─────────────────────             (this will be the publication’s ID). This is key for relational mapping.
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
        # 🔁 Loop through all authors for this publication. seq helps keep track of author order (useful for citations).
        if name_key not in author_map:  # creating author row 
            author_map[name_key] = len(author_map) + 1
            aff = auth.get("affiliation", [])
            authors.append({
                "author_id": author_map[name_key],
                "given_name": name_key[0],
                "family_name": name_key[1],
                "affiliation": aff[0]["name"] if aff else None
                }) 
        # 📌 If this author is new (based on their name), assign a new ID and record them. Grab affiliation if available.
        a_id = author_map[name_key]
        pub_auth.append({
            "publication_id": pub_idx,
            "author_id": a_id,
            "sequence": seq
            })
    return dates, sources, authors, pubs, pub_auth


# In[80]:


DB_URI


# In[64]:


engine = create_engine("mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db")
    #engine = create_engine(DB_URI, echo=False)

def create_and_load(dates, sources, authors, pubs, pub_auth):
    """Drop & recreate tables, then bulk-insert via pandas.to_sql()."""
    
    # 1) Dropping part (DDl) - Drop in reverse-dependency order . Child tables 
    # with FKs go first so you don’t violate referential integrity when dropping
    drop_order = [
         "publication_authors",
         "publications",
         "authors",
         "sources",
         "dates"
        ]
    # 2) Create tables (DDL) (parent → child)
    ddl_statements = [
        """
        CREATE TABLE dates (
          date_id INT NOT NULL,
          year INT,
          month INT,
          day INT,
          PRIMARY KEY (date_id)
        )""",
        """
        CREATE TABLE sources (
          source_id INT NOT NULL,
          source_name VARCHAR(255),
          PRIMARY KEY (source_id)
        )""",
        """
        CREATE TABLE authors (
          author_id INT NOT NULL,
          given_name VARCHAR(100),
          family_name VARCHAR(100),
          affiliation VARCHAR(255),
          PRIMARY KEY (author_id)
        )""",
        """
        CREATE TABLE publications (
          publication_id INT NOT NULL,
          doi VARCHAR(255),
          title VARCHAR(255),
          publisher VARCHAR(255),
          type VARCHAR(50),
          source_id INT,
          issued_date_id INT,
          PRIMARY KEY (publication_id),
          FOREIGN KEY (source_id) REFERENCES sources(source_id),
          FOREIGN KEY (issued_date_id) REFERENCES dates(date_id)
        )""",
        """
        CREATE TABLE publication_authors (
          publication_id INT,
          author_id INT,
          sequence INT,
          PRIMARY KEY (publication_id, author_id),
          FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
          FOREIGN KEY (author_id) REFERENCES authors(author_id)
        )"""
        ]

    with engine.begin() as conn:
        for tbl in drop_order:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
        for stmt in ddl_statements:
            conn.execute(text(stmt))
            
    
    # 3) Bulk insert (we call con=engine instead of conn, as pandas now recognizes the engine and internally calls DBAPI to do the insert)
        pd.DataFrame(dates).to_sql(name = "dates", con=engine, if_exists="append", index=False)
        pd.DataFrame(sources).to_sql(name = "sources", con=engine, if_exists="append", index=False)
        pd.DataFrame(authors).to_sql(name = "authors", con=engine, if_exists="append", index=False)
        pd.DataFrame(pubs).to_sql(name = "publications", con=engine, if_exists="append", index=False)
        pd.DataFrame(pub_auth).to_sql(name = "publication_authors", con=engine, if_exists="append", index=False)

print("All tables created and loaded successfully.")


# In[85]:


#option b with adding a raw DBAPI connection to pandas 

# hand pandas an object that actually has .cursor(). The simplest is engine.raw_connection(). 
# For example, move your inserts outside the with engine.begin() block :

engine = create_engine("mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db")
    #engine = create_engine(DB_URI, echo=False)

def create_and_load(dates, sources, authors, pubs, pub_auth):
    """Drop & recreate tables, then bulk-insert via pandas.to_sql()."""
    
    # 1) Dropping part (DDl) - Drop in reverse-dependency order Child tables 
    # with FKs go first so you don’t violate referential integrity when dropping
    drop_order = [
         "publication_authors",
         "publications",
         "authors",
         "sources",
         "dates"
        ]
    # 2) Create tables (DDL) (parent → child)
    ddl_statements = [
        """
        CREATE TABLE dates (
          date_id INT NOT NULL,
          year INT,
          month INT,
          day INT,
          PRIMARY KEY (date_id)
        )""",
        """
        CREATE TABLE sources (
          source_id INT NOT NULL,
          source_name VARCHAR(255),
          PRIMARY KEY (source_id)
        )""",
        """
        CREATE TABLE authors (
          author_id INT NOT NULL,
          given_name VARCHAR(100),
          family_name VARCHAR(100),
          affiliation VARCHAR(255),
          PRIMARY KEY (author_id)
        )""",
        """
        CREATE TABLE publications (
          publication_id INT NOT NULL,
          doi VARCHAR(255),
          title VARCHAR(255),
          publisher VARCHAR(255),
          type VARCHAR(50),
          source_id INT,
          issued_date_id INT,
          PRIMARY KEY (publication_id),
          FOREIGN KEY (source_id) REFERENCES sources(source_id),
          FOREIGN KEY (issued_date_id) REFERENCES dates(date_id)
        )""",
        """
        CREATE TABLE publication_authors (
          publication_id INT,
          author_id INT,
          sequence INT,
          PRIMARY KEY (publication_id, author_id),
          FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
          FOREIGN KEY (author_id) REFERENCES authors(author_id)
        )"""
        ]

    with engine.begin() as conn:
        for tbl in drop_order:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
        for stmt in ddl_statements:
            conn.execute(text(stmt))
            
#3  Bulk‐load via pandas, using a raw DBAPI connection (moving to-sql() part out of the with engine.begin() block)
raw = engine.raw_connection()
try:
    pd.DataFrame(dates).to_sql(
        name="dates",
        con=raw,
        if_exists="append",
        index=False
    )
    pd.DataFrame(sources).to_sql(name = "sources", con=raw, if_exists="append", index=False)
    pd.DataFrame(authors).to_sql(name = "authors", con=raw, if_exists="append", index=False)
    pd.DataFrame(pubs).to_sql(name = "publications", con=raw, if_exists="append", index=False)
    pd.DataFrame(pub_auth).to_sql(name = "publication_authors", con=raw, if_exists="append", index=False)
    raw.commit()
finally:
    raw.close()

print("All tables created and loaded successfully.")


# In[86]:


# option c:with using SQLAlchemy.Connection  

engine = create_engine("mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db")
    #engine = create_engine(DB_URI, echo=False)

def create_and_load(dates, sources, authors, pubs, pub_auth):
    """Drop & recreate tables, then bulk-insert via pandas.to_sql()."""
    
    # 1) Dropping part (DDl) - Drop in reverse-dependency order Child tables 
    # with FKs go first so you don’t violate referential integrity when dropping
    drop_order = [
        "publication_authors",
        "publications",
        "authors",
        "sources",
        "dates"
    ]
    # 2) Create tables (DDL) (parent → child)
    ddl_statements = [
        """
        CREATE TABLE dates (
          date_id INT NOT NULL,
          year INT,
          month INT,
          day INT,
          PRIMARY KEY (date_id)
        )""",
        """
        CREATE TABLE sources (
          source_id INT NOT NULL,
          source_name VARCHAR(255),
          PRIMARY KEY (source_id)
        )""",
        """
        CREATE TABLE authors (
          author_id INT NOT NULL,
          given_name VARCHAR(100),
          family_name VARCHAR(100),
          affiliation VARCHAR(255),
          PRIMARY KEY (author_id)
        )""",
        """
        CREATE TABLE publications (
          publication_id INT NOT NULL,
          doi VARCHAR(255),
          title VARCHAR(255),
          publisher VARCHAR(255),
          type VARCHAR(50),
          source_id INT,
          issued_date_id INT,
          PRIMARY KEY (publication_id),
          FOREIGN KEY (source_id) REFERENCES sources(source_id),
          FOREIGN KEY (issued_date_id) REFERENCES dates(date_id)
        )""",
        """
        CREATE TABLE publication_authors (
          publication_id INT,
          author_id INT,
          sequence INT,
          PRIMARY KEY (publication_id, author_id),
          FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
          FOREIGN KEY (author_id) REFERENCES authors(author_id)
        )"""
        ]

    with engine.begin() as sa_conn:
        for tbl in drop_order:
            sa_conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
        for stmt in ddl_statements:
            sa_conn.execute(text(stmt))
            
    # 3) Bulk insert (we call con=engine instead of conn, as pandas nwo recognizes the engine and internally calls DBAPI to to the insert)
        pd.DataFrame(dates).to_sql(name = "dates", con=sa_conn, if_exists="append", index=False)
        pd.DataFrame(sources).to_sql(name = "sources", con=sa_conn, if_exists="append", index=False)
        pd.DataFrame(authors).to_sql(name = "authors", con=sa_conn, if_exists="append", index=False)
        pd.DataFrame(pubs).to_sql(name = "publications", con=sa_conn, if_exists="append", index=False)
        pd.DataFrame(pub_auth).to_sql(name = "publication_authors", con=sa_conn, if_exists="append", index=False)

print("All tables created and loaded successfully.")


# In[65]:


print(pd.__version__)


# In[6]:


get_ipython().system('pip install pandas==1.3.5')


# In[95]:


pip install --upgrade pandas


# In[ ]:


# due to the constant fails when it comes to SQL Alchemy connectivity, i downloaded lower version fo pandas
>> pip install pandas==1.3.5
Installing collected packages: pandas
  Attempting uninstall: pandas
    Found existing installation: pandas 2.0.2


# In[87]:


# Run your pipeline interactively
raw = load_raw_items(RAW_DIR)
dates, sources, authors, pubs, pub_auth = normalize(raw)
create_and_load(dates, sources, authors, pubs, pub_auth)


# In[ ]:





# In[ ]:





# ## Transform part 

# In[ ]:




