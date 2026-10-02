from utils.spark import SparkIni
import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

yesterday = datetime.date.today() - datetime.timedelta(1)
month = yesterday.strftime('%m')
year = yesterday.strftime('%Y')
day = yesterday.strftime('%d')


def compaction():
    try:  
        connector = SparkIni("LakehouseEtl")
        spark = connector.conn()
        logger.info("reading file(s)")
        df = spark.read.csv(f"s3a://erp/raw/year={year}/month={month}/day={day}/*.csv",header=True)
        df.coalesce(1).write.mode("overwrite").parquet(f"s3a://erp/raw/consolidated/year={year}/month={month}/day={day}/")
    except Exception as e:
        logger.error(f"An error ocurred")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    compaction()
