from logging import Logger
from src.utils.config import Config

class Loader:
    """
    A class to load normalized data into Postgres database.
    """
    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.logger = logger
        self.logger.info("Loader initialized with config: %s", config.__dict__)
        self.logger.info("Loader initialized successfully.")
        self.engine = None
        self.connection = None
        self.cursor = None
        self.table_name = "crossref_data"
        self.create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            title TEXT,
            authors TEXT[],
            published_date DATE,
            doi TEXT,
            journal TEXT,
            publisher TEXT,
            is_referenced_by_count INTEGER,
            reference_count INTEGER,
        );
        """
        self.insert_query = f"""
        INSERT INTO {self.table_name} (title, authors, published_date, doi, journal, publisher, is_referenced_by_count, reference_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (doi) DO NOTHING;
        """
        self.logger.info("Loader initialized with table name: %s", self.table_name)
        self.logger.info("Loader initialized successfully.")
        self.connect_to_db()
        self.create_table()

    def connect_to_db(self):
        """
        Connect to the PostgreSQL database.
        """
        try:
            import psycopg2
            from sqlalchemy import create_engine

            # Create a connection string
            conn_string = f"postgresql://{self.config.db_user}:{self.config.db_password}@{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
            self.engine = create_engine(conn_string)
            self.connection = self.engine.connect()
            self.cursor = self.connection.cursor()
            self.logger.info("Connected to the database successfully.")
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}")

    def create_table(self):
        """
        Create the table in the database if it doesn't exist.
        """
        try:
            self.cursor.execute(self.create_table_query)
            self.connection.commit()
            self.logger.info("Table created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()
            self.logger.info("Database connection closed.")
            self.logger.info("Table creation query executed successfully.")

    def load_data(self, data):
        """
        Load normalized data into the database.
        """
        try:
            self.cursor = self.connection.cursor()
            for record in data:
                self.cursor.execute(self.insert_query, (
                    record["title"],
                    record["authors"],
                    record["published_date"],
                    record["doi"],
                    record["journal"],
                    record["publisher"],
                    record["is_referenced_by_count"],
                    record["reference_count"]
                ))
            self.connection.commit()
            self.logger.info("Data loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            self.connection.rollback()
        finally:
            self.cursor.close()
            self.connection.close()
            self.logger.info("Database connection closed.")
