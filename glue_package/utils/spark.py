import os
from pyspark.sql import SparkSession


class SparkIni:
    def __init__(self, app_name: str = "LakehouseETL"):
        self.app_name = app_name
        self.spark_conn: SparkSession | None = None

    def conn(self) -> SparkSession:
        if self.spark_conn is not None:
            return self.spark_conn

        env = os.getenv("env", "floci_windows")

        if env == "floci":
            builder = (
                SparkSession.builder.appName(self.app_name)
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                )
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
                    "spark.sql.catalog.glue_catalog.client.region", "us-east-1"
                )
                .config(
                    "spark.sql.catalog.glue_catalog.glue.endpoint",
                    "http://localhost:4566",
                )
                .config(
                    "spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
                .config(
                    "spark.sql.catalog.glue_catalog.s3.endpoint",
                    "http://localhost:4566",
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
                    "spark.sql.catalog.glue_catalog.aws.access-key-id", "floci"
                )
                .config(
                    "spark.sql.catalog.glue_catalog.aws.secret-access-key",
                    "floci",
                )
                .config(
                    "spark.hadoop.fs.s3a.endpoint", "http://localhost:4566"
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
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
            )
            self.spark_conn = builder.getOrCreate()

        elif env == "floci_mac":
            localstack_endpoint = "http://127.0.0.1:4566"

            builder = (
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
            )
            # CORREGIDO: Se asigna usando el builder configurado
            self.spark_conn = builder.getOrCreate()

        else:
            # CORREGIDO: Se asigna la sesión a self.spark_conn para AWS Glue / Cloud
            self.spark_conn = (SparkSession.builder.appName(self.app_name)
<<<<<<< HEAD
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl,org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.session.timeZone", "America/Mexico_City")
            .getOrCreate())
=======
                               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                               .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                               .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                               .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                               .config("spark.sql.session.timeZone", "America/Mexico_City").getOrCreate())
>>>>>>> origin/master

        return self.spark_conn

    def close(self) -> None:
        if self.spark_conn is not None:
            self.spark_conn.stop()
            self.spark_conn = None