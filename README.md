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

1. Connect Power BI and analyze data at gold layer
2. Model gold layers


# Useful commands to this project

zip -r ./working_file_package.zip .


# lambda
 aws lambda create-function \
    --function-name working_file_lambda \
    --runtime python3.11 \
    --role arn:aws:iam::000000000000:user/working_file_lambda \
    --handler lambda_function.lambda_handler \
    --code S3Bucket=functions,S3Key=working_file_package.zip

# step functions
    aws stepfunctions create-state-machine \
  --name "MiMaquinaDeEstadosCLI" \
  --definition "file://state_machine.json" \
  --role-arn "arn:aws:iam::000000000000:role/floci-execution-role" \
  --type "STANDARD"
# start step function
  aws stepfunctions start-execution \   
  --state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:MiMaquinaDeEstadosCLI" \                            
  --input '{"mensaje": "Hola desde la CLI"}'

aws s3 cp inventory_mock.csv s3://erp-01-01/raw/inventory/year=2026/month=10/day=13/inventory.csv



# some copy commands
aws s3 cp data/inventory_mock.csv s3://erp-01-01/raw/inventory/year=2026/month=10/day=11/inventory.csv --profile new-profile-name

