import os
from pyspark.sql import SparkSession


class SparkIni:

    def __init__(self, app_name="LakehouseETL"):
        self.app_name = app_name
        self.spark_conn = None

    def conn(self) -> SparkSession:
        env = os.getenv("AWS_PROFILE", "floci")

        if self.spark_conn is None:
            if env == "floci":
                # Usar 127.0.0.1 previene problemas de resolución IPv6 en macOS
                localstack_endpoint = "http://127.0.0.1:4566"

                self.spark_conn = (
                    SparkSession.builder.appName(self.app_name)
                    # Forzar a PySpark a vincularse al loopback IPv4 en macOS
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config(
                        "spark.jars.packages",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                        "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
                        "org.apache.hadoop:hadoop-aws:3.3.4",
                    )
                    # Configuración del Catálogo Iceberg
                    .config(
                        "spark.sql.catalog.glue_catalog",
                        "org.apache.iceberg.spark.SparkCatalog",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.catalog-impl",
                        "org.apache.iceberg.aws.glue.GlueCatalog",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.warehouse", "s3a://erp"
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.client.region",
                        "us-east-1",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.glue.endpoint",
                        localstack_endpoint,
                    )
                    # Configuración S3FileIO
                    .config(
                        "spark.sql.catalog.glue_catalog.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.s3.endpoint",
                        localstack_endpoint,
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.s3.path-style-access",
                        "true",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.aws.credentials.provider",
                        "org.apache.iceberg.aws.StaticCredentialsProvider",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.aws.access-key-id",
                        "floci",
                    )
                    .config(
                        "spark.sql.catalog.glue_catalog.aws.secret-access-key",
                        "floci",
                    )
                    # Configuración Hadoop S3A
                    .config(
                        "spark.hadoop.fs.s3a.endpoint", localstack_endpoint
                    )
                    .config("spark.hadoop.fs.s3a.access.key", "floci")
                    .config("spark.hadoop.fs.s3a.secret.key", "floci")
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config(
                        "spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    )
                    .config(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    )
                    # Extensiones Iceberg
                    .config(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    )
                    .getOrCreate()
                )
            else:
                self.spark_conn = SparkSession.builder.appName(
                    self.app_name
                ).getOrCreate()

        return self.spark_conn

    def close(self):
        if self.spark_conn is not None:
            self.spark_conn.stop()
            self.spark_conn = None