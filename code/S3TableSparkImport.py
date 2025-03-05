from pyspark.sql import SparkSession
# JDK 11
spark = SparkSession.builder \
    .appName("Iceberg Example") \
    .config("spark.driver.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.executor.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,"
            "software.amazon.awssdk:s3:2.20.0,"
            "software.amazon.awssdk:sts:2.20.0,"
            "software.amazon.awssdk:kms:2.20.0,"
            "software.amazon.awssdk:glue:2.20.0,"
            "software.amazon.awssdk:dynamodb:2.20.0,"
            "software.amazon.awssdk:s3tables:2.29.26") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "arn:aws:s3tables:us-west-2:051826712157:bucket/testtable") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
# 读取 CSV 文件
df = spark.read.option("header", "false").csv("C:/Users/Administrator/Desktop/UserBehavior.csv/UserBehavior.csv")
df = df.toDF("user_id", "item_id","item_category","behavior_type","behavior_time")
# 显示 DataFrame 确认数据已读取
df.show()

# 创建或替换临时视图
df.createOrReplaceTempView("temp_table")

# 插入数据到 Iceberg 表
spark.sql("""
INSERT INTO s3tablesbucket.testdb.commerce_shopping_big(user_id, item_id,item_category,behavior_type,behavior_time)
SELECT user_id, item_id,item_category,behavior_type,behavior_time
FROM temp_table
""")
