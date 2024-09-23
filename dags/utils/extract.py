from sqlalchemy import create_engine
import pandas as pd
from config import POSTGRES_CONN
import logging
from urllib.parse import quote_plus  # To URL-encode the password


def extract_data():
    # Get the logger for the current module
    logger = logging.getLogger(__name__)
    logger.info("Starting extract_data task.")
    try:
        logger.info("Establishing connection to PostgreSQL...")
        # Create the engine for PostgreSQL connection
        # URL-encode the password to handle special characters like '@'
        encoded_password = quote_plus(POSTGRES_CONN['password'])
        # Create the connection string with the URL-encoded password
        engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{encoded_password}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}")
        #engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}")
        logger.info("Connection established.")
        query = 'SELECT * FROM master_config."COMPLIANCE_PERIOD_MASTER"'
        logger.info(f"Executing query: {query}")
        logger.info("Extracting data...")
        activity_data = pd.read_sql(query, engine)
        logger.info("Data successfully extracted and stored in DataFrame.")
        return activity_data
    
    except Exception as e:
        logger.error(f"An error occurred during data extraction: {e}")
        raise  # Re-raise the exception to mark the task as failed

    finally:
        if 'engine' in locals():
            engine.dispose()
            logger.info("Database connection closed.")
    
 