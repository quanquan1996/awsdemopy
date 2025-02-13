from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog(
    name="glue_catalog",
    **{
        "glue.region": "us-west-2",  # 指定您的AWS区域
        "glue.id": "051826712157:s3tablescatalog/testtable",# 对应catalog id

    }
)
table = catalog.load_table("testdb.test_table")
print(table.schema())
print(table.metadata_location)
con = table.scan(
).to_duckdb(table_name="test_table")
result = con.execute("select * from test_table")
for row in result.fetchall():
    print(row)
