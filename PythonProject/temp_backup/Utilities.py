import logging
import os.path
import pandas as pd
from sqlalchemy import create_engine
from Configuration.Config import *

# Logging configuration
logging.basicConfig(
    filename="LogFile/etljob.log",# log file name
    filemode="a", # w for overwrite and a for append
    level=logging.DEBUG , # or INFO
    format ="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force = True                    # <-- this is the key
    )

logger = logging.getLogger(__name__)


mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_DATABASES}")

def check_for_duplicate_rows_in_file(file_path, file_type):
    logger.info(f"Checking for duplicate rows in file {file_path}")
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
          df = pd.read_json(file_path)
        elif file_type  ==  "xml":
          df = pd.read_xml(file_path,xpath='.//item')
        else:
          raise Exception(f"Unsupported file type passed{file_type}")

        #logger.debug(f"Loaded DataFrame with {len(df)} rows")

        duplicates = df[df.duplicated(keep=False)]

        if duplicates.empty:
            logger.info("No duplicate rows found.")
            return False
        else:
            logger.warning("\nDuplicate rows found:\n%s", duplicates.to_string(index=False))
            return True

    except Exception as e:
        logger.error("Error reading data from file", e,exc_info=True)
        raise

def check_sales_data_duplicate_column(file_path, file_type, column_name):
    logger.info(f"Checking for duplicate column value in file {file_path}")
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
          df = pd.read_json(file_path)
        elif file_type  ==  "xml":
          df = pd.read_xml(file_path,xpath='.//item')
        else:
          raise Exception(f"Unsupported file type passed{file_type}")

        #logger.debug(f"Loaded DataFrame with {len(df)} rows")

        duplicates = df[df.duplicated(subset=[column_name],keep=False)]

        if  duplicates.empty:
            logger.info("No duplicate column value found.")
            return False
        else:
            logger.warning("\nDuplicate column value  found:\n%s", duplicates)
            return True

    except Exception as e:
        logger.error("Error reading data from file", e,exc_info=True)
        raise

def check_for_null_value_in_file(file_path, file_type):
    logger.info(f"Checking for null values in file {file_path}")
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
          df = pd.read_json(file_path)
        elif file_type  ==  "xml":
          df = pd.read_xml(file_path,xpath='.//item')
        else:
          raise Exception(f"Unsupported file type passed{file_type}")

        #logger.debug(f"Loaded DataFrame with {len(df)} rows")

        duplicates = df.isnull().values.any()

        if duplicates == False:
            logger.info("No NULL Values found.")
            return False
        else:
            logger.warning("\nNULL Values found:\n%s", duplicates)
            return True

    except Exception as e:
        logger.error("Error reading data from file", e,exc_info=True)
        raise
# File existence checks
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file {file_path} does not exist {e}")


# File size checks - 0 byte file check
def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) !=0:
            logger.info("File has Data")
            return True
        else:
            logger.info("File does not have any Data")
            return False
    except Exception as e:
        logger.error(f"file {file_path} has zero byte size {e}")




if __name__ == "__main__":
    check_for_duplicate_rows_in_file(file_path, file_type)
    check_sales_data_duplicate_column(file_path, file_type,column_name)
    check_for_null_value_in_file(file_path, file_type)
    check_file_exists(file_path)
    check_file_size()






