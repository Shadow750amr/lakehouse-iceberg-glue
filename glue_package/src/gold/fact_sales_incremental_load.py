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
    spark.sql(
        '''
        MERGE INTO 
        ''')

    #then the table creation
    logging.info("Creating fact_sales table")
   
finally:
    connector.close()


