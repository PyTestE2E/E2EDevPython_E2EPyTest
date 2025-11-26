import pandas as pd
from sqlalchemy import create_engine

from CommonUtilities.Utils import read_data_from_file_write_to_stag, mysql_conn_source, logger

from Configuration.Config import *

mysql_conn_stage = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
mysql_conn_source = create_engine(
    f"mysql+pymysql://{MYSQL_USERS}:{MYSQL_PASSWORDS}@{MYSQL_HOSTS}:{MYSQL_PORTS}/{MYSQL_DATABASES}")


# data transformation scripts

def transform_filter_sales_data():
    logger.info("Sales Data Transformation started ")
    query = """SELECT * FROM stagedb.stage_sales where sale_date >= '2024-09-10'"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('sales_filter', mysql_conn_stage, if_exists='replace',index=False)
    logger.info("Sales Data Transformation completed ")

def transform_router_low_sales_data():
    logger.info("Router Transformation for Low Sales Data started ")
    query = """SELECT * FROM stagedb.sales_filter where region = 'Low'"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('sales_low', mysql_conn_stage, if_exists='replace', index=False)
    logger.info("Router Transformation for Low Sales completed")

def transform_router_high_sales_data():
    logger.info("Transformation for high Sales Data started ")
    query = """SELECT *  FROM stagedb.sales_filter where region = 'High'"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('sales_high', mysql_conn_stage, if_exists='replace', index=False)
    logger.info("Transformation for high Sales  completed")


def transform_aggr_sales_data():
    logger.info("Transformation for agg  Sales Data started ")
    query = """SELECT product_id , month(sale_date) as month ,year(sale_date) as year  , sum(price*quantity) as totalsales FROM stagedb.sales_filter group by product_id , month(sale_date),year(sale_date)"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('monthly_sales_summary', mysql_conn_stage, if_exists='replace', index=False)
    logger.info("Transformation for agg  Sales Data completed")

def transform_inventory_aggregator():
    logger.info("Inventory aggregator  summary started ")
    query = """select store_id  , sum(quantity_on_hand)  as total_inventory from stagedb.stage_inventories group by store_id"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('inventory_agg', mysql_conn_stage, if_exists='replace', index=False)
    logger.info("Inventory aggregator  summary completed")

def transform_sales_store_product_joiner():
    logger.info("transform_sales_store_product started ")
    query = """select fs.sales_id , fs.quantity,fs.price,(fs.quantity*fs.price) as total_sales_amount , fs.sale_date,p.product_id,p.product_name,s.store_id,s.store_name from stagedb.sales_filter as fs inner join stagedb.stage_products as p on fs.product_id = p.product_id inner join stagedb.stores as s on s.store_id = fs.store_id"""
    df = pd.read_sql(query, mysql_conn_stage)
    df.to_sql('sales_with_details', mysql_conn_stage, if_exists='replace', index=False)
    logger.info("transform_sales_store_product completed")

if __name__ == "__main__":
    transform_filter_sales_data()
    transform_router_low_sales_data()
    transform_router_high_sales_data()
    transform_aggr_sales_data()
    transform_inventory_aggregator()
    transform_sales_store_product_joiner()
