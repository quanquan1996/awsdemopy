# S3TableCatalog.py
# pip install "pyiceberg[pyarrow]"
import boto3
from pyiceberg.table import Table
from pyiceberg.catalog.rest import RestCatalog
import os


class S3TableCatalog:
    def __init__(self, aws_endpoint=None, region_name=None, s3_endpoint=None, table_bucket_arn=None):
        self.aws_endpoint = aws_endpoint or os.getenv('AWS_GLUE_ENDPOINT')
        self.region_name = region_name or os.getenv('AWS_DEFAULT_REGION')
        self.table_bucket_arn = table_bucket_arn
        if not self.aws_endpoint:
            raise ValueError("AWS Glue endpoint must be provided or set as AWS_GLUE_ENDPOINT environment variable")

        if not self.region_name:
            raise ValueError("AWS region must be provided or set as AWS_DEFAULT_REGION environment variable")

        self.rest_catalog = RestCatalog(
            name="my_catalog",
            uri=self.aws_endpoint,
            **{
                "rest.sigv4-enabled": "true",
                "rest.signing-region": self.region_name,
                "rest.signing-name": "glue"
            }
        )
        session = boto3.Session(region_name=self.region_name)
        self.s3table = session.client("s3tables", endpoint_url=s3_endpoint)

    def getTable(self, table_name, database_name) -> Table:
        return self.rest_catalog.load_table(database_name + "." + table_name)

    def create_namespace(self, namespace):
        self.s3table.create_namespace(tableBucketARN=self.table_bucket_arn, namespace=namespace)

    def createTable(self, table_name, database_name, schema) -> Table:
        bucket = self.s3table.get_table_bucket(tableBucketARN=self.table_bucket_arn)
        try:
            self.s3table.create_table(
                tableBucketARN=self.table_bucket_arn, namespace=database_name, name=table_name, format= "ICEBERG"
            )
        # NoSuchNamespaceError 则新建一个namespace�namespace
        except Exception as e:
            if "NoSuchNamespaceError" in str(e):
                self.create_namespace(database_name)
                self.s3table.create_table(
                    tableBucketARN=self.table_bucket_arn, namespace=database_name, name=table_name, format= "ICEBERG"
                )
            else:
                raise e
        response = self.s3tables.get_table_metadata_location(
            tableBucketARN=self.table_bucket_arn, namespace=database_name, name=table_name
        )
        warehouse_location = response["warehouseLocation"]
        return self.rest_catalog.create_table(database_name + "." + table_name, schema, location=warehouse_location)
