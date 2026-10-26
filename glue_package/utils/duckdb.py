import duckdb
import logging


logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)



class DuckDbClient:

    ''' this class open and close a duckdb connection to memory. 
    1. :memory: is important in this case.
    2. One thing to verify: if nothing is passed as parameter, then do we get the same result? 
            (It is not obviuous that when working with in-memory database and not file added then by default we work with memmory thats why)
    '''
    def __init__(self,db_path:str=":memory:"):
        self.db_path = db_path
        self.conn = None
        logger.info("Initiating duckdb connection to memory process")

    def duck_conn(self):
        self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def duck_extensions_secrets(self,cloud_enviroment:str='aws',method:str='httpfs'):
        # loading extensions
        logger.info("Installing extensions")
        self.conn.install_extension(cloud_enviroment)
        self.conn.install_extension(method)

        # and installing them
        # also can be used conn.execute(INSTALL AND LOAD aws; INSTALL AND LOAD httpfs;)
        logger.info("Loading extensions")
        self.conn.load_extension(cloud_enviroment)
        self.conn.load_extension(method)
        
        # loading secrets to current session
        # Another thing to verify: Duckdb uses a chaining credentials meaning it checks the env variables, .aws/credentials etc, whats the real order? Huge potential problems in prod env if not clarify
        self.conn.execute('''CREATE OR REPLACE SECRET secret (TYPE s3,PROVIDER credential_chain);''')
    
    def conn_close(self):
        if self.conn:
            self.conn.close()
            logger.info("Closing duckdb connection")


