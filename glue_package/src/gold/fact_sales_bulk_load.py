import pyspark.sql.functions as F
from glue_package.utils.spark import SparkIni
import sys
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


try:
    connector = SparkIni("fact_sales")
    spark = connector.conn()
    # first, filling all from sales_silver
    spark.sql(
        '''  
        INSERT INTO glue_catalog.db_gold.fact_sales (code,ticket_id,transaction_date,grand_total,pieces_sold,unit_price,cost,inventory_current_value)
            SELECT code,ticket_id,transaction_date,grand_total,pieces_sold,unit_price,null,null
                FROM glue_catalog.db_silver.sales_silver;
        '''
    )
    # secound, updating rows with new values from 
    spark.sql(
        '''  
        WITH inventory as (SELECT code,cost,inventory_current_value from glue_catalog.db_silver.inventory_silver)
        UPDATE glue_catalog.db_gold.fact_sales (cost,inventory_current_value)
        '''
    )

finally:
    connector.close()


