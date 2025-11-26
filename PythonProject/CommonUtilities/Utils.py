import pandas as pd
from sqlalchemy import create_engine
from Configuration.Config import *
import logging

# Logging configuration
logging.basicConfig(
    filename="LogFile/etljob.log",# log file name
    filemode="a", # a for overwrite and w for append
    level=logging.DEBUG , # or INFO
    format ="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

logger = logging.getLogger(__name__)


mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_DATABASES}")


def read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath='.//item')
        elif file_type == "table":
            query = f"SELECT * FROM {MYSQL_DATABASES}.{MYSQL_SOURCE_TABLES}"
            df = pd.read_sql(query, mysql_conn_source)
        else:
            raise Exception(f"Unsupported file type passed{file_type}")

        df.to_sql(table_name, db_conn, if_exists='replace', index=False)
    except Exception as e:
        print("Error reading  data from file",e)