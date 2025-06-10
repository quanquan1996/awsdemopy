import os

import duckdb
import boto3

con = duckdb.connect(database=':memory:', config={'memory_limit': '9GB','worker_threads': 5})

con.execute("""
FORCE INSTALL aws FROM core_nightly;
FORCE INSTALL httpfs FROM core_nightly;
FORCE INSTALL iceberg FROM core_nightly;
""")
result = con.execute("SELECT * FROM pragma_database_list WHERE name='temp'").fetchall()
#print(f"Home directory: {result}")
con.execute("""CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain
);""")
s3tables = boto3.client('s3tables')
table_buckets = s3tables.list_table_buckets(maxBuckets=1000)['tableBuckets']
for table_bucket in table_buckets:
    name = table_bucket['name']
    arn = table_bucket['arn']
    con.execute(
        f"""
        ATTACH '{arn}' AS {name} (
        TYPE iceberg,
        ENDPOINT_TYPE s3_tables
    );
        """
    )
def handler(event, context):
    sql = event.get("sql")
    try:
        result = con.execute(sql).fetchdf()
        return {
            "statusCode": 200,
            "result": result
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }
# event = {
#     "sql": """
#     SELECT behavior_type AS behavior_type, COUNT(user_id) AS user_count
# FROM (select a.user_id as user_id,b.category_l2 as category_l2,b.category_l1 as category_l1,a.item_id as item_id,a.behavior_type as behavior_type,a.behavior_time as behavior_time from testtable.testdb.commerce_shopping a left join  testtable.testdb.commerce_item_category b on a.item_category = b.category_l3)
# GROUP BY behavior_type
# ORDER BY user_id DESC
#     """
# }
# print(handler(event, None))