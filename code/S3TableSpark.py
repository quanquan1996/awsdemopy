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
# # 创建命名空间
# spark.sql(" CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.example_namespace")
# 创建 Iceberg 表
# spark.sql("""CREATE TABLE s3tablesbucket.testdb.commerce_shopping_test1 (
#     user_id    STRING    COMMENT '用户ID（非真实ID），经抽样&字段脱敏处理后得到',
#     item_id    STRING    COMMENT '商品ID（非真实ID），经抽样&字段脱敏处理后得到',
#     item_category    STRING    COMMENT '商品类别ID（非真实ID），经抽样&字段脱敏处理后得到',
#     behavior_type    STRING    COMMENT '用户对商品的行为类型,包括浏览、收藏、加购物车、购买，pv,fav,cart,buy)',
#     behavior_time    STRING    COMMENT '行为时间,精确到小时级别'
# ) USING iceberg""")
#
# #
# # 插入数据
# spark.sql("""
# INSERT INTO s3tablesbucket.testdb.test_table(id,data) VALUES (1,'test_connect')
# """)

# 查询数据
#spark.sql("""SELECT * FROM s3tablesbucket.testdb.test_table""").show()
#spark.sql("SELECT * FROM base1.create_demo_table1")
#spark.sql("""ALTER TABLE s3tablesbucket.testdb.test_table SET IDENTIFIER FIELDS id""")
# spark.sql(""" CREATE TABLE s3tablesbucket.testdb.orders (
#     order_id BIGINT COMMENT '订单ID',
#     user_id BIGINT COMMENT '用户ID',
#     shop_id BIGINT COMMENT '店铺ID',
#     order_time TIMESTAMP COMMENT '下单时间',
#     order_amount DECIMAL(10, 2) COMMENT '订单总金额',
#     payment_method STRING COMMENT '支付方式',
#     shipping_address STRING COMMENT '收货地址',
#     promotion_id BIGINT COMMENT '促销活动ID',
#     coupon_code STRING COMMENT '优惠券代码',
#     order_status STRING COMMENT '订单状态',
#     shipping_id BIGINT COMMENT '物流ID'
# ) USING iceberg;""")
# spark.sql(""" CREATE TABLE s3tablesbucket.testdb.order_items (
#     item_id BIGINT COMMENT '明细ID',
#     order_id BIGINT COMMENT '订单ID',
#     product_id BIGINT COMMENT '商品ID',
#     quantity INT COMMENT '购买数量',
#     unit_price DECIMAL(10, 2) COMMENT '商品单价',
#     discount_rate DECIMAL(3, 2) COMMENT '商品折扣率'
# ) USING iceberg;""")
# spark.sql("""CREATE TABLE s3tablesbucket.testdb.products (
#     product_id BIGINT COMMENT '商品ID',
#     product_name STRING COMMENT '商品名称',
#     category STRING COMMENT '商品类目',
#     sub_category STRING COMMENT '商品子类目',
#     brand STRING COMMENT '品牌',
#     supplier_id BIGINT COMMENT '供应商ID',
#     cost_price DECIMAL(10, 2) COMMENT '成本价'
# ) USING iceberg; """)
# spark.sql(""" CREATE TABLE s3tablesbucket.testdb.shops (
#     shop_id BIGINT COMMENT '店铺ID',
#     shop_name STRING COMMENT '店铺名称',
#     country_code STRING COMMENT '店铺所在国家/地区代码',
#     platform STRING COMMENT '所属平台'
# ) USING iceberg;""")
# spark.sql("""CREATE TABLE s3tablesbucket.testdb.users (
#     user_id BIGINT COMMENT '用户ID',
#     username STRING COMMENT '用户名',
#     email STRING COMMENT '邮箱',
#     country_code STRING COMMENT '用户所在国家/地区代码',
#     registration_date TIMESTAMP COMMENT '注册时间',
#     user_segment STRING COMMENT '用户分群'
# ) USING iceberg; """)
# spark.sql(""" CREATE TABLE s3tablesbucket.testdb.promotions (
#     promotion_id BIGINT COMMENT '促销活动ID',
#     promotion_name STRING COMMENT '促销活动名称',
#     promotion_type STRING COMMENT '促销类型',
#     start_date TIMESTAMP COMMENT '开始时间',
#     end_date TIMESTAMP COMMENT '结束时间',
#     discount_rule STRING COMMENT '优惠规则 (JSON)',
#     applicable_products STRING COMMENT '适用商品ID列表 (JSON)',
#     applicable_categories STRING COMMENT '适用类目列表 (JSON)'
# ) USING iceberg;""")
# spark.sql("""CREATE TABLE s3tablesbucket.testdb.shipping_info (
#     shipping_id BIGINT COMMENT '物流ID',
#     order_id BIGINT COMMENT '订单ID',
#     shipping_carrier STRING COMMENT '物流承运商',
#     tracking_number STRING COMMENT '运单号',
#     dispatch_time TIMESTAMP COMMENT '发货时间',
#     estimated_arrival TIMESTAMP COMMENT '预计送达时间',
#     actual_arrival TIMESTAMP COMMENT '实际送达时间',
#     shipping_cost DECIMAL(8, 2) COMMENT '物流成本'
# ) USING iceberg; """)
spark.sql(
    """
    CREATE TABLE s3tablesbucket.testdb.transactions (
    company_code STRING COMMENT '公司编号',
    voucher_number STRING COMMENT '凭证编号',
    voucher_posting_date STRING COMMENT '凭证过账日期',
    original_document_date STRING COMMENT '原始单据日期',
    document_type STRING COMMENT '单据类型',
    source_document_number STRING COMMENT '源单据编号',
    account_code STRING COMMENT '科目代码',
    account_name STRING COMMENT '科目名称',
    debit_amount STRING COMMENT '借方',
    credit_amount STRING COMMENT '贷方'
)
USING iceberg
    """
)
spark.sql(
    """
    CREATE TABLE s3tablesbucket.testdb.invoices (
    invoice_number STRING COMMENT '发票号',
    invoice_date STRING COMMENT '发票日期',
    amount STRING COMMENT '贷方',
    voucher_number STRING COMMENT '凭证编号',
    customer_code STRING COMMENT '客户编码'
)
USING iceberg
    """
)