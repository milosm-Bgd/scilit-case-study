from src.utils.config import Config
from src.utils.logger import setup_logger

from src.extract.extractor import Extractor
from src.preprocess.normalizer import Normalizer
from src.preprocess.deduplicator import Deduplicator
from src.load.loader import Loader

import json
from datetime import datetime

config = Config(config={
    "API_ENDPOINT": "https://api.crossref.org/works?sort=published&order=desc&rows=200",
    "DB_HOST": "localhost",
    "DB_PORT": 5432,
    "DB_NAME": "my_database",
    "DB_USER": "my_user",
    "DB_PASSWORD": "my_password",
    "LOG_FILE": "logs/app.log",
    "LOG_LEVEL": "INFO",
})

logger = setup_logger(
    name="extractor",
    log_file=config.log_file,
    level=config.log_level,
)

# API client for CrossRef works api endpoint
extract = Extractor(config, logger)
extract.fetch_and_save_data()
raw_data = extract.extract_raw_data()

normalizer = Normalizer()
normalized_data = []
for item in raw_data:
    try:
        normalized_item = normalizer.normalize(item)
        normalized_data.append(normalized_item)
    except KeyError as e:
        logger.error(f"KeyError: {e}")
        logger.error("Data format may have changed. Please check the API response.")

logger.info(f"Normalized {len(normalized_data)} items.")

# Deduplicate the data
deduplicator = Deduplicator()
unique_data = deduplicator.deduplicate(normalized_data)
logger.info(f"Deduplicated data to {len(unique_data)} items.")

# dump normalized data to a file
now = datetime.now()
filename = now.strftime("%Y%m%d_%H%M%S") + "_data.json"
filepath = f"./data/processed/{filename}"
with open(filepath, "w") as f:
    json.dump(unique_data, f, indent=4)

# Load the data into the database
loader = Loader(config, logger)
loader.load_data(unique_data)

# to do - setup dbt models and run some analysis, e.g. sum of reference_count is_referenced_by_count by journal and publisher

