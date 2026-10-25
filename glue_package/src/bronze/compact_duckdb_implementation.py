import duckdb

conn = duckdb.connect(database=":memory:")

aws_extension = conn.install_extension('aws')
conn.load_extension('aws')
conn.install_extension('httpfs')
conn.load_extension('httpfs')


conn.execute(
    '''
        CREATE OR REPLACE SECRET secret (
                TYPE s3,
                    PROVIDER credential_chain);''')

result= conn.sql("SELECT * FROM read_csv('s3://erp-01-01/raw/inventory/year=2026/month=10/day=18/inventory.csv')")

print(result)

conn.close()