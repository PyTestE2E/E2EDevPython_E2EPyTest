import pandas as pd
from sqlalchemy import create_engine , text

from CommonUtilities.Utils import read_data_from_file_write_to_stag, mysql_conn_source, logger

from Configuration.Config import *

mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}")


# data transformation scripts

def load_fact_sales_table():
    query = """ insert into targetdb.fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date) select sales_id,product_id,store_id,quantity,total_sales_amount,sale_date from stagedb.sales_with_details"""
    try:
        with mysql_conn_stage.connect() as conn:
            logger.info("Loading to fact-sales table  started....")
            logger.info(query)
            conn.execute(text(query))
            conn.commit()
            logger.info("Loading to fact-sales table completed....")
    except Exception as e:
        logger.error("error while loading in to target table fact_sales: %s",e,exc_info=True)

def load_fact_monthly_sales_summary():
    query = """insert into targetdb.fact_monthly_sales_summary (product_id , month , year ,total_sales) select product_id , month , year ,totalsales  from stagedb.monthly_sales_summary"""
    try:
        with mysql_conn_stage.connect() as conn:
            logger.info("Loading to load_fact_monthly_sales_summary  started....")
            logger.info(query)
            conn.execute(text(query))
            conn.commit()
            logger.info("Loading to load_fact_monthly_sales_summary table completed....")
    except Exception as e:
        logger.error("error while loading in to target table load_fact_monthly_sales_summary: %s",e,exc_info=True)

def load_fact_inventory_table():
    query = """insert into targetdb.fact_inventory (product_id , store_id , quantity_on_hand,last_updated) select product_id , store_id , quantity_on_hand,last_updated  from stagedb.stage_inventories"""
    try:
        with mysql_conn_stage.connect() as conn:
            logger.info("Loading to fact_inventory_table  started....")
            logger.info(query)
            conn.execute(text(query))
            conn.commit()
            logger.info("Loading to fact_inventory_table table completed....")
    except Exception as e:
        logger.error("error while loading in to target table fact_inventory_table: %s",e,exc_info=True)

def inventory_levels_by_store():
    query = """insert into targetdb.inventory_levels_by_store (store_id,total_inventory) select  store_id,total_inventory from stagedb.inventory_agg"""
    try:
        with mysql_conn_stage.connect() as conn:
            logger.info("Loading to fact_inventory_levels_by_store  started....")
            logger.info(query)
            conn.execute(text(query))
            conn.commit()
            logger.info("Loading to fact_inventory_levels_by_store completed....")
    except Exception as e:
        logger.error("error while loading in to target table fact_inventory_levels_by_store: %s",e,exc_info=True)


if __name__ == "__main__":
    load_fact_sales_table()
    load_fact_inventory_table()
    load_fact_monthly_sales_summary()
    inventory_levels_by_store()


