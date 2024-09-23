# import pandas as pd # type: ignore
# import snowflake.connector # type: ignore
# from snowflake.connector.pandas_tools import write_pandas # type: ignore
# from config import SNOWFLAKE_CONN
 
# def load_data(activity_data):
#     conn = snowflake.connector.connect(
#         user=SNOWFLAKE_CONN['user'],
#         password=SNOWFLAKE_CONN['password'],
#         account=SNOWFLAKE_CONN['account'],
#         warehouse=SNOWFLAKE_CONN['warehouse'],
#         database=SNOWFLAKE_CONN['database'],
#         schema=SNOWFLAKE_CONN['schema']
#     )
#     write_pandas(conn, activity_data, 'activity_assignment_archive')
#     conn.close()

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import logging
from config import SNOWFLAKE_CONN  # Import the Snowflake connection details from config

def load_data(**kwargs):
    # Set up logger
    logger = logging.getLogger(__name__)
    logger.info("Starting load_data task.")

    # Retrieve the transformed data from XCom
    logger.info("Fetching transformed data from XCom...")
    task_instance = kwargs['ti']
    activity_data = task_instance.xcom_pull(task_ids='transform_data')

    if isinstance(activity_data, pd.DataFrame):
        logger.info(f"Data received for loading, shape: {activity_data.shape}")

        try:
            #  Establish connection to Snowflake
            logger.info("Connecting to Snowflake...")
            # conn = snowflake.connector.connect(
            #     user=SNOWFLAKE_CONN['user'],
            #     password=SNOWFLAKE_CONN['password'],
            #     account=SNOWFLAKE_CONN['account'],
            #     warehouse=SNOWFLAKE_CONN['warehouse'],
            #     database='GRC_MASTER',        # Your specific database
            #     schema='MASTER_CONFIG',       # Your specific schema
            # )
            # Retrieve Snowflake connection details from Airflow
            snowflake_conn = BaseHook.get_connection('snowflake_conn')
 
            # Establish connection to Snowflake
            #logger.info("Connecting to Snowflake...")
            
            conn = snowflake.connector.connect(
                user=snowflake_conn.login,
                password=snowflake_conn.password,
                #account=snowflake_conn.host,  # Snowflake account identifier
                account=snowflake_conn.extra_dejson.get('account'),  
                warehouse=snowflake_conn.extra_dejson.get('warehouse'),  # Warehouse from extras
                database=snowflake_conn.extra_dejson.get('database'),  # Database from extras
                schema=snowflake_conn.schema,  # Database from the schema field
                
            )
            logger.info("Connection to Snowflake established.")
            
            # Write the DataFrame to the specified Snowflake table
            logger.info("Writing data to Snowflake table 'COMPLIANCE_PERIOD_MASTER'...")
            success, nchunks, nrows, _ = write_pandas(conn, activity_data, 'COMPLIANCE_PERIOD_MASTER')

            # Close the Snowflake connection
            conn.close()
            logger.info("Snowflake connection closed.")
            
            # Log success or failure
            if success:
                logger.info(f"Successfully loaded {nrows} rows into Snowflake table 'ASSIGNMENT_MASTER'.")
            else:
                logger.error("Failed to load data into Snowflake.")
        
        except Exception as e:
            logger.error(f"An error occurred while loading data into Snowflake: {e}")
            raise  # Re-raise the exception to mark the task as failed

    else:
        logger.error("Expected a DataFrame, but received something else.")
        raise ValueError("Expected a DataFrame, but received something else.")

