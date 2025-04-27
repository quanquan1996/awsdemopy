from pyiceberg.catalog import load_catalog
import pyarrow as pa

rest_catalog = load_catalog(
    "catalog_name",
    **{
        "type": "rest",
        "warehouse": "arn:aws:s3tables:us-west-2:051826712157:bucket/testtable",
        "uri": "https://s3tables.us-west-2.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": "us-west-2",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    }
)

# 新建namespace
rest_catalog.create_namespace("namespace_example")
# 新建表
rest_catalog.create_table(
    "namespace_example.test_table",
    schema=pa.schema(
        [
            ("id", pa.int32()),
            ("data", pa.string()),
        ]
    )
)
# 打印表列表
tables_list = rest_catalog.list_tables("namespace_example")
print(tables_list)
# 获取表对象
table = rest_catalog.load_table("namespace_example.test_table")
df = pa.Table.from_pylist(
    [{"id": 303, "data": 'test insert icb'}], schema=table.schema().as_arrow()
)
#插入表
table.append(df)
# 读取表并打印
for row in table.scan().to_arrow().to_pylist():
    print(row)
