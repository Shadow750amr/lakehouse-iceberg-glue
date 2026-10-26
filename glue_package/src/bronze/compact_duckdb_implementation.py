import duckdb
from awsglue.utils import getResolvedOptions
import sys


options = getResolvedOptions(sys.argv,['JOB_NAME','partition_day','partition_month','partition_year'])

day = options["partition_day"]
month = options["partition_month"]
year = options["partition_year"]




class DuckDbClient:

    ''' this class open and close a duckdb connection to memory. 
    1. :memory: is important in this case.
    2. One thing to verify: if nothing is passed as parameter, then do we get the same result? 
            (It is not obviuous that when working with in-memory database and not file added then by default we work with memmory thats why)
    3. to do: modularised this class
    '''
    def __init__(self,db_path:str=":memory:"):
        self.db_path = db_path
        self.conn = None

    def duck_conn(self):
        self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def duck_extensions_secrets(self,cloud_enviroment:str='aws',method:str='httpfs'):
        # loading extensions
        self.conn.install_extension(cloud_enviroment)
        self.conn.install_extension(method)

        # and installing them
        # also can be used conn.execute(INSTALL AND LOAD aws; INSTALL AND LOAD httpfs;)
        self.conn.load_extension(cloud_enviroment)
        self.conn.load_extension(method)
        
        # loading secrets to current session
        # Another thing to verify: Duckdb uses a chaining credentials meaning it checks the env variables, .aws/credentials etc, whats the real order? Huge potential problems in prod env if not clarify
        self.conn.execute('''CREATE OR REPLACE SECRET secret (TYPE s3,PROVIDER credential_chain);''')
    
    def conn_close(self):
        if self.conn:
            self.conn.close()
            print("Conexión a DuckDB cerrada automáticamente.")





def compaction():
    '''this function reads and save to parquet both sales and inventory usign above class'''

    try:
        # initializing class
        client = DuckDbClient(":memory:")
        # reading files
        inventory = client.duck_conn().sql(f"SELECT * FROM read_csv('s3://erp-01-01/raw/inventory/year={year}/month={month}/day={day}/inventory.csv')")
        sales = client.duck_conn().sql(f"SELECT * FROM read_csv('s3://erp-01-01/raw/sales/year={year}/month={month}/day={day}/sales_warehouse.csv')")

        # loading into parquet 
        inventory.write_parquet(f"s3://erp-01-01/consolidated/inventory/year={year}/month={month}/day={day}/inventory.parquet")
        sales.write_parquet(f"s3://erp-01-01/consolidated/sales/year={year}/month={month}/day={day}/sales_warehouse.parquet")

        # setting extensions and secrets
        client.duck_extensions_secrets('aws','httpfs')

    finally:
        client.conn_close()

