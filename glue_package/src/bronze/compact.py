from glue_package.utils.spark import SparkIni
import datetime
from zoneinfo import ZoneInfo
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# cdmx time zone
cdmx_tz = ZoneInfo("America/Mexico_City")
today = datetime.datetime.now(cdmx_tz).date()
yesterday = today - datetime.timedelta(days=1)

month = yesterday.strftime('%m')
year = yesterday.strftime('%Y')
day = yesterday.strftime('%d')

def compaction():
    try:  
        connector = SparkIni("LakehouseEtl")
        spark = connector.conn()
        logger.info(f"Reading file(s) for partition: year={year}/month={month}/day={day}")
        
        df = spark.read.csv(f"s3://erp-01-01/raw/inventory/year={year}/month={month}/day={day}/", header=True)
        
       
        df.coalesce(1).write.mode("overwrite").parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/")
        
        logger.info("Compaction finished successfully")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    compaction()