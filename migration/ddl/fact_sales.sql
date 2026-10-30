CREATE OR REPLACE TABLE fact_sales (
        code STRING COMMENT 'Internal id code',
            ticket_id STRING COMMENT 'system origin key for transaction',
                transaction_date TIMESTAMP COMMENT 'system origin date for transaction',
                    grand_total DECIMAL(18, 6) COMMENT 'grand total for that transaction / total amount paid',
                        pieces_sold INT COMMENT 'pieces sold in that specific transaction',
                            unit_price DECIMAL(18, 6) COMMENT 'unit price of the product',
                                cost DECIMAL(18, 6) COMMENT 'Current cost (no conversion applied)',
                                    inventory_current_value DECIMAL(18, 6) COMMENT 'Actual value in inventory')
    COMMENT 'Fact sales table that consolidates both sales and inventory into one governed table with doc schema'
    PARTITIONED BY (day(transaction_date))
    LOCATION 's3://erp-01-01/gold/fact_sales'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet');