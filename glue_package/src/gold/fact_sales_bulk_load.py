import pyspark.sql.functions as F
from glue_package.utils.spark import SparkIni
import sys
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


try:
    connector = SparkIni("fact_sales")
    spark = connector.conn()
    logging.info("Initiating process")
    # droping table allows no mistakes during execution 
    logging.info("Dropping fact_sales table")
    spark.sql("DROP TABLE IF EXISTS glue_catalog.db_gold.fact_sales;")

    #then the table creation
    logging.info("Creating fact_sales table")
    spark.sql(
        ''' 
    CREATE TABLE IF NOT EXISTS glue_catalog.db_gold.fact_sales (
                    code STRING COMMENT 'Internal id code',
                    ticket_id STRING COMMENT 'system origin key for transaction',
                    transaction_date TIMESTAMP COMMENT 'system origin date for transaction',
                    grand_total DECIMAL(18,6) COMMENT 'grand total for that transaction / total amount paid',
                    pieces_sold INT COMMENT 'pieces sold in that specific transaction',
                    unit_price DECIMAL(18,6) COMMENT 'unit price of the product',
                    cost DECIMAL(18,6) COMMENT 'Current cost (no conversion applied)',
                    inventory_current_value DECIMAL(18,6) COMMENT 'Actual value in inventory')
        USING iceberg
            PARTITIONED BY (days(transaction_date))
                COMMENT 'Fact sales table that consolidates both sales and inventory into one governed table with doc schema';''')

    # finally inserting rows by selecting and joining, first approach was inserting + merging but added overhead :P
    # Cost and inventory current value are actually part of the dim_product and fact_inventory tables
    logging.info("Filling fact_sales table")
    spark.sql(

        '''
        INSERT INTO glue_catalog.db_gold.fact_sales (
            code,
            ticket_id,
            transaction_date,
            grand_total,
            pieces_sold,
            unit_price,
            cost,
            inventory_current_value
        )
        SELECT 
            s.code,
            s.ticket_id,
            s.transaction_date,
            s.grand_total,
            s.pieces_sold,
            s.unit_price,
            i.cost,
            i.inventory_current_value
        FROM glue_catalog.db_silver.sales_silver s
        LEFT JOIN glue_catalog.db_silver.inventory_silver i
            ON s.code = i.code;
    '''

    )
    logging.info("Creating dim_product table")

    spark.sql(
            ''' 
        CREATE TABLE IF NOT EXISTS glue_catalog.db_gold.dim_product (
                        code STRING COMMENT 'Internal id code',
                        cost DECIMAL(18,6) COMMENT 'Current cost (no conversion applied)',
                        description STRING COMMENT  "Product description",
                        manufacterer STRING,
                        abc_code STRING,
                        catalog_code STRING
                        )
            USING iceberg
                    COMMENT 'dim product table';''')
    logging.info("filling dim product table")
    # inventory_silver had no duplicates otherwise this would require row_number over partition by to reomve duplicates.
    spark.sql(
        '''
        INSERT INTO glue_catalog.db_gold.dim_product (code,cost,description,manufacterer,abc_code,catalog_code)
        SELECT code,cost,description,manufacterer,abc_code,catalog_code FROM glue_catalog.db_silver.inventory_silver;
        
        ''')

finally:
    connector.close()


