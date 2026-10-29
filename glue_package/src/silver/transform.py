from glue_package.utils.spark import SparkIni
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
import sys
import logging
import yaml

logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Contract for inventory silver
with open('glue_package/utils/inventory_contract.yml',"r", encoding="utf-8") as file:
    inventory_contract = yaml.safe_load(file)
    inventory_types_dict = {item['source_name']: item['data_type'] for item in inventory_contract['schema']}
    inventory_names_dict = {item['source_name']: item['target_name'] for item in inventory_contract['schema']}
# Contract for sales silver

with open('glue_package/utils/sales_contract.yml',"r", encoding="utf-8") as file:
    sales_contract = yaml.safe_load(file)
    sales_types_dict = {item['source_name']: item['data_type'] for item in sales_contract['schema']}
    sales_names_dict = {item['source_name']: item['target_name'] for item in sales_contract['schema']}

def transformation():
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
        logger.info("Initiating transformation phase")

        connector = SparkIni("LakehouseETL")
        spark = connector.conn()

        logger.info("Reading inventory df")
        # READING inventory dataframe
        inventory_df = spark.read.parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/")
        
        logger.info("Casting date and converting data types to inventory df")
        #definining expressions to iterate over type dictionary and cast columns
        inventory_types_exprs = [F.col(col_name).cast(target_type).alias(col_name)
                            if col_name in inventory_types_dict else F.col(col_name)
                                for col_name, target_type in inventory_types_dict.items()]

        #applying dictionary to change data types
        inventory_casted_df = inventory_df.select(*inventory_types_exprs)
        
        logger.info("Applying naming convention with contract to inventory df")
        #renaming columns with names dictionary (config settings: glue 5.0 or above - las versiones anteriores no funcionan :] )
        inventory_renamed_df = inventory_casted_df.withColumnsRenamed(inventory_names_dict)


        logger.info("Writing final output")
        #writing to final destination
        inventory_renamed_df.writeTo("glue_catalog.db_silver.inventory_silver").using("iceberg").createOrReplace()


        logger.info("Reading sales df")
        sales_df = spark.read.parquet(f"s3://erp-01-01/consolidated/sales/year={year}/month={month}/day={day}/")

        sales_types_exprs = [F.col(col_name).cast(target_type).alias(col_name)
                            if col_name in sales_types_dict else F.col(col_name)
                                for col_name, target_type in sales_types_dict.items()]
                                
        logger.info("Casting sales df")
        sales_casted_df = (sales_df.withColumn("fecha",F.date_format(F.to_date(F.col("fecha_venta"), "dd/MM/yyyy"), "yyyy/MM/d")).select(*sales_types_exprs))
        
        logger.info("Renaming sales df")
        # renaming sales dataframe
        sales_renamed_df = sales_casted_df.withColumnsRenamed(sales_names_dict)

        logger.info("Writing sales df into destination")
        sales_renamed_df.writeTo("glue_catalog.db_silver.sales_silver").using("iceberg").createOrReplace()

    except Exception as e:
        logger.error(f"An error ocurred")
        raise e
    finally:
        connector.close()

if __name__ == "__main__":
    transformation()

    
