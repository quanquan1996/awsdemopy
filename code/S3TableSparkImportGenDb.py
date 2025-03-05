from pyspark.sql import SparkSession

spark = (SparkSession.builder \
    .appName("Glue Catalog Example") \
    .config("spark.driver.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.executor.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.sql.catalog.gluecatalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gluecatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.gluecatalog.warehouse", "s3://qpj-tables/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.gluecatalog.clientFactory", "org.apache.iceberg.aws.glue.GlueClientFactory") \
    .config("spark.sql.catalog.gluecatalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.gluecatalog.glue-client.factory.type", "default") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1," 
            "software.amazon.awssdk:s3:2.20.0," 
            "software.amazon.awssdk:sts:2.20.0," 
            "software.amazon.awssdk:kms:2.20.0," 
            "software.amazon.awssdk:glue:2.20.0," 
            "software.amazon.awssdk:dynamodb:2.20.0")
         .getOrCreate())

# 读取 CSV 文件
df = spark.read.option("header", "false").csv("C:/Users/Administrator/Desktop/UserBehavior.csv/UserBehavior.csv")
df = df.toDF("user_id", "item_id","item_category","behavior_type","behavior_time")

# 显示 DataFrame 确认数据已读取
df.show()

# 创建或替换临时视图
df.createOrReplaceTempView("temp_table")

# 插入数据到 Glue Catalog 表
spark.sql("""
INSERT INTO  gluecatalog.gendb.commerce_shopping_big(user_id, item_id,item_category,behavior_type,behavior_time)
SELECT user_id, item_id,item_category,behavior_type,behavior_time
FROM temp_table
""")
