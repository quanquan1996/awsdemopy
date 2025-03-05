import urllib.parse
import boto3
import time
import gzip


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

        # 检查文件是否是gzip格式
        if key.endswith('.gz'):
            # 读取gzip压缩内容
            content = gzip.decompress(response['Body'].read()).decode('utf-8')
        else:
            # 读取普通内容
            content = response['Body'].read().decode('utf-8')

        return content

    except Exception as e:
        print(f"Error reading S3 file {s3_path}: {e}")
        return None


def lambda_handler(event, context):
    # 从event中获取参数
    region_name = event.get('region_name', 'us-west-2')
    application_id = event.get('application_id', '')  # EMR Serverless应用程序ID
    query = event.get('code', '')
    sql = event.get('sql', '')
    job_role_arn = event.get('job_role_arn', '')  # 执行作业的IAM角色
    s3_logs_bucket = event.get('s3_logs_bucket', '')  # 存储日志的S3桶
    s3_output_bucket = event.get('s3_output_bucket', '')  # 存储输出的S3桶

    if sql != '':
        query = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("running_spark_job").getOrCreate()
df = spark.sql(\"\"\"{sql}\"\"\")
rows = df.collect()
row_dicts = [row.asDict() for row in rows]
import json
print(json.dumps(row_dicts))"""

    # 创建 EMR Serverless 客户端
    emr_serverless = boto3.client('emr-serverless', region_name=region_name)

    # 将Spark代码写入临时文件并上传到S3
    s3_client = boto3.client('s3')
    script_key = f"scripts/spark_job_{int(time.time())}.py"
    s3_client.put_object(
        Bucket=s3_output_bucket,
        Key=script_key,
        Body=query.encode('utf-8')
    )
    script_location = f"s3://{s3_output_bucket}/{script_key}"

    # 配置Spark作业参数
    spark_submit_params = {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    }

    # 转换为命令行参数格式
    spark_submit_args = []
    for key, value in spark_submit_params.items():
        spark_submit_args.append(f"--conf")
        spark_submit_args.append(f"{key}={value}")

    # 启动EMR Serverless作业
    response = emr_serverless.start_job_run(
        applicationId=application_id,
        executionRoleArn=job_role_arn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': script_location,
                'sparkSubmitParameters': ' '.join(spark_submit_args)
            }
        },
        configurationOverrides={
            'monitoringConfiguration': {
                'cloudWatchLoggingConfiguration': {
                    'enabled': True
                },
                's3MonitoringConfiguration': {
                    'logUri': f"s3://{s3_logs_bucket}/logs/"
                }
            }
        },
        name=f"SparkJob-{int(time.time())}",
        executionTimeoutMinutes=600  # 设置超时时间
    )

    job_run_id = response['jobRunId']
    print(f"Started EMR Serverless job with ID: {job_run_id}")

    # 等待作业完成
    job_state = 'SUBMITTED'
    while job_state in ['SUBMITTED', 'PENDING', 'SCHEDULED', 'RUNNING']:
        time.sleep(10)  # 每10秒检查一次状态
        response = emr_serverless.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id
        )
        job_state = response['jobRun']['state']
        print(f"Job state: {job_state}")

        if job_state in ['FAILED', 'CANCELLED']:
            error_message = response['jobRun'].get('stateDetails', 'Unknown error')
            print(f"Job failed: {error_message}")
            return {"error": f"Job failed: {error_message}"}

    # 作业完成，获取结果
    if job_state == 'SUCCESS':
        # 获取作业日志位置
        log_prefix = f"logs/{application_id}/{job_run_id}/jobs/driver/stdout"

        # 列出日志文件
        try:
            response = s3_client.list_objects_v2(
                Bucket=s3_logs_bucket,
                Prefix=log_prefix
            )

            # 读取结果文件
            result_content = ""
            if 'Contents' in response:
                for obj in response['Contents']:
                    result_s3_path = f"s3://{s3_logs_bucket}/{obj['Key']}"
                    content = read_s3_path_to_string(result_s3_path)
                    if content:
                        result_content += content

                return result_content
            else:
                # 尝试使用另一种路径格式
                log_prefix = f"logs/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout"
                response = s3_client.list_objects_v2(
                    Bucket=s3_logs_bucket,
                    Prefix=log_prefix
                )

                if 'Contents' in response:
                    for obj in response['Contents']:
                        result_s3_path = f"s3://{s3_logs_bucket}/{obj['Key']}"
                        content = read_s3_path_to_string(result_s3_path)
                        if content:
                            result_content += content

                    return result_content
                else:
                    return {"error": "Could not find output logs in S3"}
        except Exception as e:
            print(f"Error retrieving job output: {e}")
            return {"error": f"Error retrieving job output: {e}"}

    return {"error": "Job did not complete successfully"}


# 测试代码 - 取消注释即可运行
# 定义各个参数
region_name = 'us-west-2'
application_id = '00fqmvjdt26l1q0l'  # 替换为你的EMR Serverless应用程序ID
job_role_arn = 'arn:aws:iam::051826712157:role/Admin'  # 替换为你的IAM角色ARN
s3_logs_bucket = 'qpj-emr-temp-uswest2'  # 替换为你的日志桶
s3_output_bucket = 'qpj-emr-temp-uswest2'  # 替换为你的输出桶
sql = "SELECT count(*) FROM testdb.test_table"

# 组合成event对象
event = {
    'region_name': region_name,
    'application_id': application_id,
    'job_role_arn': job_role_arn,
    's3_logs_bucket': s3_logs_bucket,
    's3_output_bucket': s3_output_bucket,
    'sql': sql
}

# 创建一个空的context对象
context = {}

# 调用lambda_handler函数
result = lambda_handler(event, context)

# 打印结果
print("Test result:", result)
