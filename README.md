### ETL THROUGH LAKEHOUSE USING ICEBERG TABLE OPEN FORMAT AND GLUE AS CATALOG (MEDALLION)
The main purpose of this project is to desing and build a **lakehouse** and develop an ETL pipeline using mocked transactional data from warehouse erp (inventory and sales).
The benefits of this approach is to leverage the modern data architecture using cloud enviroments to analyze data at rest through datalakehouse approach.
The main idea is to model this data using a medallion architecture and kimball approach in gold layer.

## Tools used for this project

Important to say this project was implemented using **https://floci.io/**, a local emulator for cloud enviroments that works well with most functions and integrations for serverless projects prototypes. Also, it uses the AWS CLI that pretty much is the most essential needed toll for dev and test functions, buckets and IAM configurations.

* **Glue catalog:**
* **S3:** Amazon S3 stores raw transactional data in a partitioned hive-style folder structure (`year=YYYY/month=MM/`) to be compacted and then ingested into silver.
* **Athena:** Adhoc analyze and main connector to external BI tools.



## Next steps and possible new features
 ###
1. Connect Power BI and analyze data at gold layer
2. Model gold layers