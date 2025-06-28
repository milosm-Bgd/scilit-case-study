import os
import glob
import json
import requests
from datetime import datetime
from logging import Logger
from requests.exceptions import HTTPError
from tqdm import tqdm

from src.utils.config import Config

class Extractor:
    """
    DataLoader class for loading data from CrossRef API.

    Attributes:
        config (Config): Configuration object containing API endpoint and key.
        headers (Dict[str, str]): Headers for the API request.
    """

    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.headers = {
            "Accept": "application/json",
        }

        self.logger = logger
        self.logger.info("DataLoader initialized with config: %s", config.__dict__)
        self.logger.info("Headers set for API request: %s", self.headers)
        self.logger.info("DataLoader initialized successfully.")

    # todo - function to fetch data from CrossRef API by looping through the pages
    def fetch_and_save_data(self):
        self.logger.info("Fetching data from CrossRef API...")
        self.logger.error("Looping API results to be implemented...")

        # Example of how to use tqdm for progress bar
        # for i in tqdm(range(100), desc="Loading", unit="item"):
        #     time.sleep(0.1)  # Simulate some work being done
        #     self.logger.debug("Progress: %d%%", (i + 1) * 100 / 100)

        # Fetching some data
        try:
            response = requests.get(self.config.api_endpoint, headers=self.headers)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            self.logger.info("Data fetched successfully from API.")
        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")

        # dump raw json response data to a file
        now = datetime.now()
        filename = now.strftime("%Y%m%d_%H%M%S") + "_data.json"
        filepath = f"./data/raw/{filename}"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)

    def extract_raw_data(self):
        """
        Extract raw data from the json files in ./data/raw directory.
        """
        self.logger.info("Extracting raw data from JSON files...")

        data = []
        json_files = glob.glob(os.path.join("./data/raw", "*.json"))
        for file in tqdm(json_files, desc="Extracting data", unit="file"):
            with open(file, "r") as f:
                try:
                    json_data = json.load(f)

                    # only get json_data.items
                    if "message" in json_data and "items" in json_data["message"]:
                        items = json_data.get("message", {}).get("items", [])
                        for item in items:
                            if isinstance(item, dict):
                                data.append(item)
                            else:
                                self.logger.warning(f"Item is not a dictionary: {item}")
                    else:
                        self.logger.warning(f"No 'message' or 'items' found in file {file}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error decoding JSON from file {file}: {e}")
                    continue

        self.logger.info("Raw data extraction completed.")
        return data
