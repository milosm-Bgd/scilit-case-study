# Case Study: Data Engineering

---

**⚠️ PLEASE DO NOT FORK THIS REPO AS OTHERS MAY SEE YOUR CODE. INSTEAD YOU CAN USE THE
[USE THIS TEMPLATE](https://github.com/new?template_name=case-study-data-engineering&template_owner=MDPI-AG)
BUTTON TO CREATE YOUR OWN REPOSITORY.**

---

## Targeted Workflow

Extract → Store raw → Preprocess → Load to PostgreSQL → Transform with dbt → Analyze

## Getting Started

Install the required packages using pip. It is recommended to use a virtual environment
to avoid conflicts with other projects.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the Postgres database using Docker:

```bash
docker-compose up -d
```

This will start a Postgres database on port 5432. You can access the database using
any Postgres client.

## Issues with CrossRef API

If you face any issues with accessing the CrossRef API, you can add `&mailto=your@email`
into the URL. This way CrossRef assigns your API request to a prioritized pool. Use
a real email address.

## Suggested Project Structure

```plaintext
.
├── data                         # Local storage for ingested data
│   ├── raw                     # Raw dumps from API
│   └── processed               # Cleaned/preprocessed files (if needed)
│
├── src                         # All Python source code
│   ├── extract                 # Code to call APIs and fetch raw data
│   │   ├── __init__.py
│   │   └── extractor.py
│   │
│   ├── preprocess              # Normalize / clean / deduplicate raw data
│   │   ├── __init__.py
│   │   └── normalize.py
│   │
│   ├── load                    # Load preprocessed data into Postgres
│   │   ├── __init__.py
│   │   └── loader.py
│   │
│   ├── utils                   # Config, logging, etc.
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── logger.py
│   │
│   └── pipeline.py             # Orchestrates all the steps end-to-end
│
├── dbt                         # dbt project directory
│   ├── models
│   │   ├── staging             # Raw to cleaned staging models
│   │   └── marts               # Final models / business logic
│   ├── seeds
│   └── snapshots
│
├── docker-compose.yml          # Docker Compose file to run Postgres
├── main.py                     # Entrypoint that runs the pipeline
├── README.md
└── requirements.txt
```

## Some Ideas for Todos

- improve the fetching of raw data: loop a few pages of the API responses
- improve the deduplicator logic: some items with different DOI's may belong together (e.g., a preprint and a journal article version of the same work)
- start the dbt schema and sql models to run some analytics on the data, e.g., sum citations per year, or per journal, or per publisher
- decompose the `main.py` entrypoints into distinct pipelines and use an orchestrator such as Airflow or Prefect
- Dockerize the python app

## dbt

If you are not familiar with dbt, you can check their [sandox project](https://github.com/dbt-labs/jaffle-shop/)
on GitHub to get started. You can also check the [dbt documentation](https://docs.getdbt.com/docs/introduction)
for more information.
