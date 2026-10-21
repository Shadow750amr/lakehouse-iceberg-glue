from glue_package.utils.spark import SparkIni
import logging
from awsglue.utils import getResolvedOptions
import sys
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def compaction():
    '''
    Executes a compaction job against raw files (csv files) and convert them into parquet based files
    in a conssolidated path (hive like)'''
    try:  
        connector = SparkIni("LakehouseEtl")
        spark = connector.conn()
        # args handler to be inyected via cli
        args = getResolvedOptions(sys.argv, ["JOB_NAME","partition_day","partition_month","partition_year"])
        day = args["partition_day"]
        month = args["partition_month"]
        year =  args["partition_year"]

        logger.info(f"Reading file(s) for partition: {year}-{month}-{day}")
        
        df = spark.read.csv(f"s3://erp-01-01/raw/inventory/year={year}/month={month}/day={day}/inventory.csv", header=True)
        
        df.coalesce(1).write.mode("overwrite").parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/")
        
        logger.info("Compaction finished successfully")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    compaction()