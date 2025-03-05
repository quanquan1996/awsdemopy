import boto3
import time


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

    # 返回任务ID和其他必要信息，以便第二个函数使用
    return {
        "job_run_id": job_run_id,
        "application_id": application_id,
        "s3_logs_bucket": s3_logs_bucket,
        "region_name": region_name
    }


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

# 调用submit_job_handler函数
result = lambda_handler(event, context)
print(result)