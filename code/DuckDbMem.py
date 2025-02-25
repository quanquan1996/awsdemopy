import boto3
import io
import duckdb
import urllib.parse
import pyarrow as pa
import pyarrow.parquet as pq

def parse_s3_uri(s3_uri):
    """解析 S3 URI 为 bucket 和 key"""
    parsed = urllib.parse.urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key

def query_s3_parquet_with_pyarrow(s3_uris, sql_query):
    """
    从 S3 下载 Parquet 文件到内存，使用 PyArrow 读取，然后用 DuckDB 查询
    """
    s3_client = boto3.client('s3')
    tables = []

    # 下载并解析所有文件
    for uri in s3_uris:
        bucket, key = parse_s3_uri(uri)
        buffer = io.BytesIO()
        s3_client.download_fileobj(bucket, key, buffer)
        buffer.seek(0)

        # 使用 PyArrow 读取 Parquet 文件
        table = pq.read_table(buffer)
        tables.append(table)

    # 合并所有表
    if tables:
        combined_table = pa.concat_tables(tables)

        # 创建 DuckDB 连接并注册 PyArrow 表
        con = duckdb.connect(database=':memory:')
        con.register('all_data', combined_table)

        # 执行查询
        modified_query = sql_query.replace("parquet_files", "all_data")
        result = con.execute(modified_query).fetchall()
        print(result)
        return result
    else:
        return None

# 使用示例
s3_uris = [
    's3://f5765277-96d2-46a7-ux8ujutra4xt9446bn9x8ntwxurqsusw2b--table-s3/data/00000-0-2b308b74-5e18-4eb0-8ea2-72b7dadab7c7-0-00001.parquet',
    's3://f5765277-96d2-46a7-ux8ujutra4xt9446bn9x8ntwxurqsusw2b--table-s3/data/00001-1-2b308b74-5e18-4eb0-8ea2-72b7dadab7c7-0-00001.parquet'
]

# 执行查询
result_df = query_s3_parquet_with_pyarrow(s3_uris, "SELECT COUNT(*) FROM parquet_files LIMIT 10")
print(result_df)
