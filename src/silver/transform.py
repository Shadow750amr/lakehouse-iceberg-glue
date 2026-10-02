from utils.spark import SparkIni
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, TimestampType, DateType, StringType
import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

yesterday = datetime.date.today() - datetime.timedelta(1)
month = yesterday.strftime('%m')
year = yesterday.strftime('%Y')
day = yesterday.strftime('%d')

schema_dict = {"cost":DecimalType(18,6),
                "description":StringType(),
                        "manufacterer":StringType(),
                            "abc_code":StringType(),
                                "catalog_code": StringType(),
                                 "inventory_current_value":DecimalType(18,6),
                                 "creation_date": TimestampType(),
                                  "ingestion_date": TimestampType()}

def run_silver():
    try:  
        connector = SparkIni("LakehouseETL")
        spark = connector.conn()

        logger.info("reading file(s)")

        initial_df = spark.read.parquet(f"s3a://erp/raw/consolidated/year={year}/month={month}/day={day}/")

        select_exprs = [F.col(col_name).cast(target_type).alias(col_name) 
                            if col_name in schema_dict else F.col(col_name)
                                for col_name, target_type in schema_dict.items()]
        casted_date = (initial_df.withColumn("",F.to_date(F.col("fecha"),"yyyyMMdd")).select(*select_exprs))
        
        casted_date.writeTo("glue_catalog.inventory.inventory_silver").using("iceberg").createOrReplace()
    except Exception as e:
        logger.error(f"An error ocurred")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    run_silver()

    
