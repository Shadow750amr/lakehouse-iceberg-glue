import pyspark.sql.functions as F
from glue_package.utils.spark import SparkIni
import sys
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


try:
    connector = SparkIni("fact_sales")
    spark = connector.conn()
    inventory_df = spark.read.table("glue_catalog.db_silver.inventory_silver")
    sales_df = spark.read.table("glue_catalog.db_silver.sales_silver")
    sales_df.join(inventory_df,"catalog_code").show()
finally:
    connector.close()


