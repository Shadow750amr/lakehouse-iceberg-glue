# ETL THROUGH LAKEHOUSE USING ICEBERG TABLE OPEN FORMAT AND GLUE AS CATALOG (MEDALLION)

The main purpose of this project is to desing and build a **lakehouse** and develop an ETL pipeline using mocked transactional data from warehouse erp (inventory and sales).
The benefits of this approach is to leverage the modern data architecture using cloud enviroments to analyze data at rest through datalakehouse approach.
The main idea is to model this data using a medallion architecture and kimball approach in gold layer.

> ## Tools used for this project

Important to say this project was implemented using **https://floci.io/**, a local emulator for cloud enviroments that works well with most functions and integrations for serverless projects prototypes.

More ahead in the project I migrated this to AWS services.

* Glue catalog: Main catalog and base for the iceberg table.
* Glue etl jobs: Powers the pyspark scripts through serverless executions.
* S3: Amazon S3 stores raw transactional data in a partitioned hive-style folder structure (`year=YYYY/month=MM/`) to be compacted and then ingested into silver.
* Athena: Adhoc analyze and main connector to external BI tools.
* Power BI: Visualization tools for reporting and BI.
* Event Bridge: Runtime execution tool for scheduled-run of step function workflow.
* Step function: Orchestration tool.
* Docker: Getting the project working with spark and glue utils.

> ## Challenges of this project

This section is intended to be a transparent exercise of what challenges I have been through this project in order to:
1. Teach others about the inherent workload by working with this such tools and;
2. Leaving a track for further configurations.

**Docker as starting point** : So starting with the very basics I strongly recommend using docker for developing because you can test whatever pyspark 
job you want with the agrregate of using glue utils such as getResolvedOption, otherwise you would have to use argparser from python making it a little bit more complicated to debug a test. Nevertheless, for initiate workloads and simple tests, Floci works well with it. Also, if using AWS services, work in a dev enviroment!
**Glue versions**: Make sure you work with the right glue version (such as 5.0) if using some metods such as withColumnsRenamed otherwise the jobs will faill.
**StateMachines**: Some state machines may work well Jsonata but for this project Jsonpath was the right one and no more configurations were added. This being said, make sure to pass the arguments through states with $$.Execution.Input. otherwise this will fail. Also, add the .sync prefix to the resource in order to succesfully execute the states in a syncronus way.




> ## Next steps and possible new features

1. Connect Power BI and analyze data at gold layer
2. Model gold layers


> ## Useful commands to this project

> ### general

zip -r ./working_file_package.zip .

> ### lambda
 aws lambda create-function \
    --function-name working_file_lambda \
    --runtime python3.11 \
    --role arn:aws:iam::000000000000:user/working_file_lambda \
    --handler lambda_function.lambda_handler \
    --code S3Bucket=functions,S3Key=working_file_package.zip
	
> ### step functions

```bash
aws stepfunctions create-state-machine \
  --name "MiMaquinaDeEstadosCLI" \
  --definition "file://state_machine.json" \
  --role-arn "arn:aws:iam::000000000000:role/floci-execution-role" \
  --type "STANDARD"
```

```bash
  aws stepfunctions start-execution \   
  --state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:MiMaquinaDeEstadosCLI" \                            
  --input '{"mensaje": "Hola desde la CLI"}'
```

> ### s3
```bash 
aws s3 cp inventory_mock.csv s3://erp-01-01/raw/inventory/year=2026/month=10/day=13/inventory.csv
```
```bash
aws s3 cp data/inventory_mock.csv s3://erp-01-01/raw/inventory/year=2026/month=10/day=11/inventory.csv --profile new-profile-name
```
> ### spark
```bash
spark-submit glue_package/src/bronze/compact.py --partition_year 2026 --partition_month 10 --partition_day 18
eval $(aws configure export-credentials --format env)
```