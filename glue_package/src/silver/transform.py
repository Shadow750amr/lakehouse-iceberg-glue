from glue_package.utils.spark import SparkIni
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
import sys
import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



with open('glue_package/utils/contract.yml',"r", encoding="utf-8") as file:
    contract = yaml.safe_load(file)
    types_dict = {item['source_name']: item['data_type'] for item in contract['schema']}
    names_dict = {item['source_name']: item['target_name'] for item in contract['schema']}
    

def run_silver():
    '''
    This function is the tranformation phase of the ETL and consist in the following steps:
    - Casts fecha column into yyyy/mm/dd format
    - Change data types into specific format using a contract defined in yaml (utils module)
    - Rename dataframe into specific naming using the same contract defined in yaml.
    '''
    args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'partition_day',
                           'partition_month',
                           'partition_year'])

    day = args["partition_day"]
    month = args["partition_month"]
    year = args["partition_year"]

    try:  
        connector = SparkIni("LakehouseETL")
        spark = connector.conn()

        logger.info("reading file(s)")

        #reading initial df
        initial_df = spark.read.parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/")
        
        #definining expressions to iterate over type dictionary and cast columns
        types_exprs = [F.col(col_name).cast(target_type).alias(col_name)
                            if col_name in types_dict else F.col(col_name)
                                for col_name, target_type in types_dict.items()]

        #applying date formating and using type dictionary to change data types
        casted_df = (initial_df.withColumn("fecha",F.date_format(F.to_date(F.col("fecha"), "dd/MM/yyyy"), "yyyy/mm/dddd")).select(*types_exprs))

        #renaming columns with names dictionary
        
        renamed_df = casted_df.withColumnsRenamed(names_dict)

        #writing to final destination
        renamed_df.writeTo("mi_catalogo.db_inventory_silver.inventory_silver").using("iceberg").createOrReplace()

    except Exception as e:
        logger.error(f"An error ocurred")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    run_silver()

    
