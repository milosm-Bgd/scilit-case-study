# ### ETL for the case study

# #### src/ingest.py

# In[2]:

import os
import json
import requests
from datetime import datetime



RAW_DIR = "data/raw/"
URL = "https://api.crossref.org/works"
PARAMS = {"sort": "published", "order": "desc", "rows": 200}


# In[48]:



# In[10]:


def fetch_crossref():
    resp = requests.get(URL, params=PARAMS, timeout=30)
    resp.raise_for_status()
    return resp.json()["message"]["items"]


# In[8]:


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


items = fetch_crossref() # calling first function 
save_raw(items)  # calling second function on items we created by first function 


# #### src/transform_load.py

# In[13]:

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import sqlalchemy


# ######    ───── CONFIG  part   ───── ───── ───── ───── ───── ───── ───── ─────

# In[15]:

# 1) Point this at your MySQL DW (replace placeholders):
DB_URI = "mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db"


# In[16]:


DB_URI


# In[19]:


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


# In[59]:


#new validaiotn script , shorter
from sqlalchemy import create_engine

eng = create_engine("mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db")
with eng.connect() as conn:
    print(conn.execute("SELECT 1").scalar())


# In[34]:


validate_mysql_connection(USER, PASSWORD, HOST, DATABASE)


# In[34]:


os.getcwd()


# In[44]:


# 2) Raw JSON folder (created by ingest.py)
RAW_DIR = os.chdir("C:\\Users\\milos\\data\\raw")
# RAW_DIR = os.path.join(os.path.dirname(os.getcwd()), "\\data\\raw")


# In[36]:


# Table names
TABLES = ["dates", "sources", "authors", "publications", "publication_authors"]


# In[37]:


TABLES


# ######  ───── FUNCTIONS part ────────────────────────────────────────────────────────────
# 

# In[38]:


def load_raw_items(raw_dir):# imamo praznu listu, zatim kreiramo putanju gde nam je prvi deo apstrakcija/ direktorijum raw_dir 
    """Read every .json in data/raw/ into a list of dicts."""#  na koji nadovezujemo ime objekta unutar direktorijuma
    items = [] # zatim otvaramo context manager sa with open() dajemo mu apsktakciju f , citamo sadrzaj contexta file-like objecta 
    for fn in os.listdir(raw_dir):#koji zatim uvcitavamo u native python object , items tj lista
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
        #counter = 1 
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
                #"id": counter,
                "publication_id": pub_idx,
                "author_id": a_id,
                "sequence": seq
                }) 
            #counter += 1
    return dates, sources, authors, pubs, pub_auth


# In[40]:


DB_URI


# In[99]:


# option a: normal connection + modified DDL
engine = create_engine("mysql+pymysql://Milosh_85:Nikoljdan2021@127.0.0.1:3306/scilit_db")
#engine = create_engine(DB_URI, echo=False)
def create_and_load(dates, sources, authors, pubs, pub_auth):
    # 1) Dropping part (DDl) - Drop in reverse-dependency order Child tables 
    # with FKs go first so you don’t violate referential integrity when dropping
    drop_order = [
        "publication_authors",
        "publications",
        "authors",
        "sources",
        "dates"
    ]
    ddl_statements = [
    """
    USE scilit_db;
    """,
    """
    DROP TABLE IF EXISTS dates;
    """,
    """
    CREATE TABLE dates (
     date_id INT NOT NULL,
     year INT,
     month INT,
     day INT,
     PRIMARY KEY (date_id)
    );
    """,
    """
    DROP TABLE IF EXISTS sources;
    """,
    """
    CREATE TABLE sources (
     source_id INT NOT NULL,
     source_name VARCHAR(255),
     PRIMARY KEY (source_id)
    );
    """,
    """
    DROP TABLE IF EXISTS authors;
    """,
    """
    CREATE TABLE authors (
     author_id INT NOT NULL,
     given_name VARCHAR(100),
     family_name VARCHAR(100),
     affiliation VARCHAR(255),
     PRIMARY KEY (author_id)
    );
    """,
    """
    DROP TABLE IF EXISTS publications;
    """,
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
    );
    """,
    """
    DROP TABLE IF EXISTS publication_authors;
    """,
    """
    CREATE TABLE publication_authors (
     id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
     publication_id INT,
     author_id INT,
     sequence INT,
     FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
     FOREIGN KEY (author_id) REFERENCES authors(author_id)
    );"""
    ]

    with engine.begin() as conn:
        for tbl in drop_order:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl};"))  #make the comments talked to NIkola on the tomorrow meeting 
        for stmt in ddl_statements:
            conn.execute(text(stmt))
    
        pd.DataFrame(dates).to_sql(name = "dates", con=conn, if_exists="append", index=False)
        pd.DataFrame(sources).to_sql(name = "sources", con=conn, if_exists="append", index=False)
        pd.DataFrame(authors).to_sql(name = "authors", con=conn, if_exists="append", index=False)
        pd.DataFrame(pubs).to_sql(name = "publications", con=conn, if_exists="append", index=False)
        pd.DataFrame(pub_auth).to_sql(name = "publication_authors", con=conn, if_exists="append", index=False)

print("All tables created and loaded successfully.")



RAW_DIR = "C:\\Users\\milos\\data\\raw"


# ### Run your pipeline interactively 

# In[62]:


# calling our first function , purpose is loading the list out of the direcory containing the JSON files content 
# (practically preparing for laodng into dataframes )
raw = load_raw_items(RAW_DIR)


#calling our second function normalize(raw)
dates, sources, authors, pubs, pub_auth = normalize(raw)


len(dates), len(sources), len(authors), len(pubs), len(pub_auth)



#callng our third function : create_and_load()
create_and_load(dates, sources, authors, pubs, pub_auth)

# In[57]:

dates, sources, authors, pubs, pub_auth




# ## Transform part 

# In[ ]:




