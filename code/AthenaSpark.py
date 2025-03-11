import urllib
import boto3
import time


def parse_s3_uri(s3_uri):
    """解析 S3 URI 为 bucket 和 key"""
    parsed = urllib.parse.urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def read_s3_path_to_string(s3_path):
    bucket, key = parse_s3_uri(s3_path)

    # 初始化S3客户端
    s3_client = boto3.client('s3')

    try:
        # 获取S3对象
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # 读取内容并解码为字符串
        content = response['Body'].read().decode('utf-8')

        return content

    except Exception as e:
        print(f"Error reading S3 file: {e}")
        return None


def lambda_handler(event, context):
    # 从event中获取参数
    region_name = event.get('region_name', 'us-west-2')
    glue_id = event.get('catalog_id', '')
    query = event.get('code', '')
    sql = event.get('sql', '')
    work_group = event.get('work_group', 'pyspark')
    if sql != '':
        query = f"""
df = spark.sql(\"\"\"{sql}\"\"\")
rows = df.collect()
print(rows)"""
    # 创建 Athena 客户端
    athena_client = boto3.client('athena', region_name=region_name)

    # 步骤 1: 创建 Spark 会话
    response = athena_client.start_session(
        WorkGroup=work_group,  # 你创建的 Spark 工作组名称
        EngineConfiguration={
            'CoordinatorDpuSize': 1,  # 可选，默认为1
            'MaxConcurrentDpus': 20,  # 可选，设置最大并发 DPU
            'DefaultExecutorDpuSize': 1,  # 可选，默认执行器 DPU 大小
            'SparkProperties': {
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.spark_catalog.glue.id": glue_id,
                "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            }
        }
    )

    # 获取会话 ID
    session_id = response['SessionId']
    print(f"Created Spark session with ID: {session_id}")

    # 等待会话变为 IDLE 状态
    session_state = 'CREATING'
    while session_state not in ['IDLE', 'FAILED', 'TERMINATED']:
        response = athena_client.get_session(
            SessionId=session_id
        )
        session_state = response['Status']['State']
        print(f"Session state: {session_state}")
        if session_state in ['FAILED', 'TERMINATED']:
            print(f"Session failed to start: {response['Status']['StateChangeReason']}")
            return {"error": f"Session failed to start: {response['Status']['StateChangeReason']}"}
        if session_state != 'IDLE':
            time.sleep(5)  # 等待5秒再检查

    # 步骤 2: 在会话中执行查询
    if session_state == 'IDLE':
        response = athena_client.start_calculation_execution(
            SessionId=session_id,
            Description='My Spark SQL query',  # 可选
            CodeBlock=query
        )

        calculation_id = response['CalculationExecutionId']
        print(f"Started calculation with ID: {calculation_id}")

        # 步骤 3: 等待查询完成并获取结果
        calculation_state = 'CREATING'
        while calculation_state in ['CREATING', 'QUEUED', 'RUNNING']:
            response = athena_client.get_calculation_execution(
                CalculationExecutionId=calculation_id
            )
            calculation_state = response['Status']['State']
            print(f"Calculation state: {calculation_state}")
            if calculation_state in ['FAILED', 'CANCELLED']:
                print(f"Calculation failed: {response}")
                return {"error": f"Calculation failed: {response}"}
            if calculation_state != 'COMPLETED':
                time.sleep(2)  # 等待2秒再检查

        # 获取结果
        if calculation_state == 'COMPLETED':
            # 处理结果
            results = response['Result']
            print("Query results:", results)
            s3Uri = results['StdOutS3Uri']
            result_content = read_s3_path_to_string(s3Uri)
            print("Query results:", result_content)
            return result_content

    return {"error": "Session or calculation did not complete successfully"}

    # test


# # 定义各个参数
# region_name = 'us-west-2'
# glue_id = '051826712157:s3tablescatalog/testtable'
# query = """
# df = spark.sql(\"\"\"SELECT count(*) FROM testdb.test_table\"\"\")
# rows = df.collect()
# row_dicts = [row.asDict() for row in rows]
# import json
# print(json.dumps(row_dicts))"""
#
# # 组合成event对象
# event = {
#     'region_name': region_name,
#     'glue.id': glue_id,
#     'query': query
# }
#
# # 创建一个空的context对象
# context = {}
#
# # 调用handle函数
# result = handle(event, context)
#
# # 打印结果
# print("Test result:", result)
