from S3TableCatalog import S3TableCatalog

region = "us-east-1"
catalog = S3TableCatalog(
    aws_endpoint=f"https://glue.{region}.amazonaws.com/iceberg",
    region_name=region,
    s3_endpoint=f"https://s3.{region}.amazonaws.com",
    table_bucket_arn="arn:aws:s3tables:us-east-1:051826712157:bucket/qpj-user"
)


def test_get_data():
    table = catalog.getTable("city_2", "base")
    print(table.scan().to_arrow())


def test_get_data_pandas():
    table = catalog.getTable("city_2", "base")
    print(table.scan().to_pandas())

def test_create_table():
    table = catalog.createTable("create_demo_table", "base", {"city": "string", "population": "int"})
    print(table.schema())
