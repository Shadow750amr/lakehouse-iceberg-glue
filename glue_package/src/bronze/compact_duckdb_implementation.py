from glue_package.utils.duckdb import DuckDbClient
from awsglue.utils import getResolvedOptions
import sys
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


options = getResolvedOptions(sys.argv,['JOB_NAME','partition_day','partition_month','partition_year'])

day = options["partition_day"]
month = options["partition_month"]
year = options["partition_year"]


def compaction():
    '''this function reads and save to parquet both sales and inventory usign above class'''

    try:
        # initializing class
        logger.info("Initiating process")
        client = DuckDbClient(":memory:")

        # reading files
        logger.info("Reading files")
        inventory = client.duck_conn().sql(f"SELECT * FROM read_csv('s3://erp-01-01/raw/inventory/year={year}/month={month}/day={day}/inventory.csv')")
        sales = client.duck_conn().sql(f"SELECT * FROM read_csv('s3://erp-01-01/raw/sales/year={year}/month={month}/day={day}/sales_warehouse.csv')")

        # loading into parquet 
        logger.info("Converting files into parquet and uploading")
        inventory.write_parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/inventory.parquet")
        sales.write_parquet(f"s3://erp-01-01/consolidated/sales/year={year}/month={month}/day={day}/sales_warehouse.parquet")

        # setting extensions and secrets
        client.duck_extensions_secrets('aws','httpfs')

        logger.info(f"Process succesfully completed for compaction job")

    except Exception as e:
        logger.error(f"Error with process: {e}")
        raise e

    finally:
        client.conn_close()


if __name__ == "__main__":
    compaction()
