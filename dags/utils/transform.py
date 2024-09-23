# import pandas as pd # type: ignore
# from datetime import datetime
 
# def transform_data(activity_data):
#     activity_data['archived_at'] = datetime.now()
#     return activity_data


import pandas as pd
from datetime import datetime
import logging

def transform_data(**kwargs):
    # Set up logging for the transform_data task
    logger = logging.getLogger(__name__)
    logger.info("Starting transform_data task.")

    # Use context to pull activity_data from XCom
    logger.info("Attempting to pull activity_data from XCom.")
    task_instance = kwargs['ti']
    activity_data = task_instance.xcom_pull(task_ids='extract_data')

    # Ensure the pulled data is a DataFrame, not a string
    if isinstance(activity_data, pd.DataFrame):
        logger.info("Successfully pulled activity_data as a DataFrame.")
        # # Add the 'archived_at' column
        # logger.info("Adding 'archived_at' column to activity_data DataFrame.")
        # activity_data['archived_at'] = datetime.now()
        # logger.info("Successfully added 'archived_at' column.")
        # Add the 'ARCHIVED_AT' column with DATE type (without time)
        logger.info("Adding 'ARCHIVED_AT' column to activity_data DataFrame with DATE type.")
        #activity_data['ARCHIVED_AT'] = pd.to_datetime(datetime.now()).dt.
        #activity_data['ARCHIVED_AT'] = datetime.now()
        # Set the ARCHIVED_AT column to the current timestamp and format it as Snowflake expects
        activity_data['ARCHIVED_AT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Match Snowflake TIMESTAMP_NTZ(9)
        logger.info("Successfully added 'ARCHIVED_AT' column with DATE type.")
        #logger.info(f"ARCHIVED_AT: {activity_data['ARCHIVED_AT']}")
        # Log column names and their data types
        logger.info(f"Column names: {list(activity_data.columns)}")
        logger.info(f"Column data types:\n{activity_data.dtypes}")

        # Log the first few rows of the DataFrame (to avoid excessive logging)
        logger.info(f"Sample data:\n{activity_data.head()}")

        # Optionally, you can log all values (but this may be too verbose for large DataFrames)
        #logger.info(f"Full data:\n{activity_data.to_string()}")

        # Log each column's values
        for column in activity_data.columns:
            logger.info(f"Values in column '{column}':\n{activity_data[column].tolist()}")



        # Convert data types to match Snowflake schema
        logger.info("Converting column data types to match Snowflake schema.")                
        # Ensure ID and similar columns are integers
        activity_data['ID'] = activity_data['ID'].astype('int64')
        activity_data['STANDARD_ID'] = activity_data['STANDARD_ID'].astype('int64')
        activity_data['ASSIGNMENT_CREATED'] = activity_data['ASSIGNMENT_CREATED'].astype('int64')
        activity_data['LICENSE_ID'] = activity_data['LICENSE_ID'].astype('int64')                
        # Convert dates from string/object to datetime format, assuming they are in the correct string format
        #activity_data['COMPLIANCE_START_DATE'] = pd.to_datetime(activity_data['COMPLIANCE_START_DATE'],format='%Y-%m-%d')
        #activity_data['COMPLIANCE_END_DATE'] = pd.to_datetime(activity_data['COMPLIANCE_END_DATE'],format='%Y-%m-%d')                
        # Ensure BOOLEAN columns are properly formatted
        activity_data['IS_ACTIVE'] = activity_data['IS_ACTIVE'].astype('bool') 
        activity_data['IS_OPEN'] = activity_data['IS_OPEN'].astype('bool')
        activity_data['ARCHIVE'] = activity_data['ARCHIVE'].astype('bool')

        # Log column names and their data types
        logger.info(f"Column names after transformation: {list(activity_data.columns)}")
        logger.info(f"Column data types after transformation:\n{activity_data.dtypes}")
        
        return activity_data
    else:
        logger.error("Failed to pull a DataFrame. Received unexpected data type.")
        raise ValueError("Expected a DataFrame, but received something else.")
