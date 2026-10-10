from utils.spark import SparkIni
from pyspark.sql import functions as F
import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

yesterday = datetime.date.today() - datetime.timedelta(2)
month = yesterday.strftime('%m')
year = yesterday.strftime('%Y')
day = yesterday.strftime('%d')


def run_silver():
    try:  
        connector = SparkIni("LakehouseETL")
        spark = connector.conn()
        logger.info("reading file(s)")
        df = spark.read.format("iceberg").load("glue_catalog.inventory.inventory_silver")
        df.show()
    except Exception as e:
        logger.error(f"An error ocurred")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    run_silver()

#df.writeTo("glue_catalog.inventory.inventory_silver").using("iceberg").create
    
#("CREATE OR REPLACE TABLE glue_catalog.inventory.inventory(codigo STRING,descripcion STRING,costo DECIMAL,fabricante INT,abc DECIMAL,cat STRING,valor_inv_ACTUAL DECIMAL,FECHA timestamp) USING ICEBERG PARTITIONED BY (fecha);")
          