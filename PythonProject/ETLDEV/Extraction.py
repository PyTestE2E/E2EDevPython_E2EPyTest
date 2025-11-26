import pandas as pd
from sqlalchemy import create_engine

from CommonUtilities.Utils import read_data_from_file_write_to_stag, mysql_conn_source, logger

from Configuration.Config import *

mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_DATABASES}")



def extract_sales_datafile_load_stage_table(file_path, file_type, table_name, db_conn):
    logger.info("Extracting Sales Data File to Load Stage Table")
    try:
        read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    except Exception as e:
        logger.error(f"Error while reading data from file: {e}", exc_info=True)




def extract_product_datafile_load_stage_table(file_path, file_type, table_name, db_conn):
    logger.info("Extracting product Data File to Load Stage Table")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    logger.info("Loaded product Data File Load Stage Table Complete")


def extract_supplier_datafile_load_stage_table(file_path, file_type, table_name, db_conn):
    logger.info("Extracting supplier Data File to Load Stage Table")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    logger.info("Loaded supplier Data File Load Stage Table Complete")


def extract_inventory_datafile_load_stage_table(file_path, file_type, table_name, db_conn):
    logger.info("Extracting inventory Data File to Load Stage Table")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    logger.info("Loaded inventory Data File Load Stage Table Complete")


def extract_stores_datafile_load_stage_table(file_path, file_type, table_name, db_conn):
    logger.info("Extracting stores Data File to Load Stage Table")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    logger.info("Loaded stores Data File Load Stage Table Complete")


if __name__ == "__main__":
    extract_sales_datafile_load_stage_table("SourceData/sales_data.csv", "csv", "stage_sales", mysql_conn_stage)
    extract_product_datafile_load_stage_table("SourceData/product_data.csv", "csv", "stage_products", mysql_conn_stage)
    extract_supplier_datafile_load_stage_table("SourceData/supplier_data.json", "json", "stage_suppliers", mysql_conn_stage)
    extract_inventory_datafile_load_stage_table("SourceData/inventory_data.xml", "xml", "stage_inventories", mysql_conn_stage)
    extract_stores_datafile_load_stage_table("mysql_conn_source", "table", "stores", mysql_conn_stage)

