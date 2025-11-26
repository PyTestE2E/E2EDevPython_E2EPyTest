# dq_checks.py
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from Configuration.Config import *
# Ensure Log folder
os.makedirs("LogFile", exist_ok=True)

# Logging configuration
logging.basicConfig(
    filename="LogFile/etljob.log",
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True
)

mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_DATABASES}")

mysql_con_target = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_TDATABASES}")

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Helper: Load file to DataFrame
# -------------------------------------------------------------------
def load_file(file_path, file_type):
    try:
        if file_type == "csv":
            return pd.read_csv(file_path)

        elif file_type == "json":
            return pd.read_json(file_path)

        elif file_type == "xml":
            return pd.read_xml(file_path, xpath=".//item")

        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}", exc_info=True)
    raise


# -------------------------------------------------------------------
# File-level DQ checks
# -------------------------------------------------------------------
def check_file_exists(file_path):
    try:
        exists = os.path.isfile(file_path)
        if exists:
            logger.info(f"File exists: {file_path}")
        else:
            logger.error(f"File NOT found: {file_path}")
        return exists
    except Exception as e:
        logger.error(f"Error checking file: {e}", exc_info=True)
        raise


def check_file_size(file_path):
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 0:
            logger.info(f"File has data: {file_path}")
            return True
        else:
            logger.error(f"File is empty: {file_path}")
            return False
    except Exception as e:
        logger.error(f"Error getting file size: {e}", exc_info=True)
        raise

def check_file_no_duplicates(file_path, file_type, subset=None):
    df = load_file(file_path, file_type)
    duplicates = df[df.duplicated(subset=subset, keep=False)]

    if duplicates.empty:
        logger.info("No duplicate rows found.")
        return False
    else:
        logger.warning(f"Duplicate rows found:\n{duplicates.to_string(index=False)}")
        return True

def check_file_duplicate_column(file_path, file_type, column):
    df = load_file(file_path, file_type)
    duplicates = df[df.duplicated(subset=[column], keep=False)]

    if duplicates.empty:
        logger.info(f"No duplicate values in column {column}.")
        return False
    else:
        logger.warning(f"Duplicate values in {column}:\n{duplicates}")
        return True

def check_file_null_values(file_path, file_type):
    df = load_file(file_path, file_type)
    has_null = df.isnull().values.any()

    if not has_null:
        logger.info("No NULL values found.")
        return False
    else:
        logger.warning("NULL values found in file.")
        return True

def load_sql(query, mysql_conn_source):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logger.error(f"SQL execution error: {query} | {e}", exc_info=True)
        raise


def check_table_no_duplicate_keys(query, key_cols, conn):
    df = pd.read_sql(query, mysql_conn_source)
    duplicates = df[df.duplicated(subset=key_cols, keep=False)]

    if duplicates.empty:
        logger.info("No duplicate keys found.")
        return False
    else:
        logger.error(f"Duplicate keys found:\n{duplicates}")
        return True

def check_column_not_null(query, column, mysql_con_target):
    df = pd.read_sql(query, mysql_con_target)
    null_rows = df[df[column].isnull()]

    if null_rows.empty:
        logger.info(f"No NULL values in {column}.")
        return False
    else:
        logger.error(f"NULL values found in column {column}:\n{null_rows}")
        return True

def check_row_count_match(source_query, target_query, mysql_conn_source ,mysql_con_target, tolerance =0):
    df_source = pd.read_sql(source_query, mysql_conn_source)
    df_target = pd.read_sql(target_query, mysql_con_target)

    diff = abs(len(df_source) - len(df_target))

    if diff == tolerance:
        logger.info("Row count check passed.")
        return False
    else:
        logger.error(f"Row count mismatch: diff={diff}, tolerance={tolerance}")
        return True

def check_referential_integrity(source_query, target_query, COLUMN_NAME, mysql_conn_source ,mysql_con_target):
    df_source = pd.read_sql(source_query, mysql_conn_source)
    df_target = pd.read_sql(target_query, mysql_con_target)

    missing = df_target[~df_target[COLUMN_NAME].isin(df_source[COLUMN_NAME])]

    if missing.empty:
        logger.info("Referential integrity OK.")
        return False
    else:
        logger.error(f"Orphan keys found:\n{missing}")
        missing.to_csv("Data_Difference/extra_sales_id.in_fact_sales_table.csv", index=False)
        return True
