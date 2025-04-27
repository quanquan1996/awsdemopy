from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, expr, rand, floor, date_add, to_timestamp
from datetime import datetime, timedelta
import random

# Iceberg catalog 和 database 配置
CATALOG_NAME = "s3tablesbucket"
DATABASE_NAME = "testdb"

# 表名
ORDERS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.orders"
ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.order_items"
PRODUCTS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.products"
SHOPS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.shops"
USERS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.users"
PROMOTIONS_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.promotions"
SHIPPING_INFO_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.shipping_info"

# 初始化 SparkSession (确保已配置 Iceberg)
spark = SparkSession.builder \
    .appName("IcebergDataInitializationSQL") \
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
    .enableHiveSupport() \
    .getOrCreate()

# Helper function to generate random timestamp within a range
def random_timestamp(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").timestamp()
    end = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").timestamp()
    return datetime.fromtimestamp(random.uniform(start, end))

sql_statements = []

# -------------------- 生成基础数据 SQL --------------------

# 用户数据
num_users = 100
for i in range(1, num_users + 1):
    user_id = i
    username = f"user_{i}"
    email = f"user_{i}@example.com"
    country_code = random.choice(["SG", "US", "CN"])
    registration_date = random_timestamp("2024-01-01 00:00:00", "2025-03-20 23:59:59").strftime('%Y-%m-%d %H:%M:%S')
    user_segment = random.choice(["New", "Regular", "VIP"])
    sql = f"""
        INSERT INTO {USERS_TABLE} (user_id, username, email, country_code, registration_date, user_segment)
        VALUES ({user_id}, '{username}', '{email}', '{country_code}', '{registration_date}', '{user_segment}')
    """
    sql_statements.append(sql)

# 店铺数据
num_shops = 50
for i in range(1, num_shops + 1):
    shop_id = i
    shop_name = f"shop_{i}"
    country_code = random.choice(["US", "CN", "JP", "SG"])
    platform = random.choice(["Amazon", "eBay", "Shopify"])
    sql = f"""
        INSERT INTO {SHOPS_TABLE} (shop_id, shop_name, country_code, platform)
        VALUES ({shop_id}, '{shop_name}', '{country_code}', '{platform}')
    """
    sql_statements.append(sql)

# 商品数据
num_products = 20
categories = ["Electronics", "Clothing", "Home Goods", "Books", "Beauty"]
brands = ["BrandA", "BrandB", "BrandC", "Generic"]
product_costs = {}  # 用于存储商品 ID 和成本价
for i in range(1, num_products + 1):
    product_id = i
    product_name = f"product_{i}"
    category = random.choice(categories)
    sub_category = f"sub_category_{random.choice(categories)}"
    brand = random.choice(brands)
    supplier_id = random.randint(1, 5)
    cost_price = round(random.uniform(10, 200), 2)
    product_costs[product_id] = cost_price  # 存储商品成本价
    sql = f"""
        INSERT INTO {PRODUCTS_TABLE} (product_id, product_name, category, sub_category, brand, supplier_id, cost_price)
        VALUES ({product_id}, '{product_name}', '{category}', '{sub_category}', '{brand}', {supplier_id}, {cost_price})
    """
    sql_statements.append(sql)

# 促销活动数据
promotions_data = [
    (1, "Spring Sale", "Discount", "2025-03-10 00:00:00", "2025-03-17 23:59:59", '{"type": "percentage", "value": 0.1}', '[1, 3, 5, 7]', '["Electronics", "Clothing"]'),
    (2, "Summer Flash Sale", "Fixed Price", "2025-03-25 00:00:00", "2025-03-28 23:59:59", '{"type": "fixed", "value": 50, "threshold": 200}', '[2, 4, 6]', '["Home Goods"]'),
    (3, "Clearance", "Buy One Get One Free", "2025-02-01 00:00:00", "2025-02-15 23:59:59", '{"type": "bogo"}', '[8, 9]', '["Books"]'), # 已过期
    (4, "VIP Discount", "Percentage", "2025-03-15 00:00:00", "2025-04-01 23:59:59", '{"type": "percentage", "value": 0.05}', None, None), # 适用所有
]
for promo in promotions_data:
    promotion_id, promotion_name, promotion_type, start_date_str, end_date_str, discount_rule, applicable_products, applicable_categories = promo
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
    applicable_products_str = f"'{applicable_products}'" if applicable_products else 'NULL'
    applicable_categories_str = f"'{applicable_categories}'" if applicable_categories else 'NULL'
    sql = f"""
        INSERT INTO {PROMOTIONS_TABLE} (promotion_id, promotion_name, promotion_type, start_date, end_date, discount_rule, applicable_products, applicable_categories)
        VALUES ({promotion_id}, '{promotion_name}', '{promotion_type}', '{start_date}', '{end_date}', '{discount_rule}', {applicable_products_str}, {applicable_categories_str})
    """
    sql_statements.append(sql)

# -------------------- 生成订单和物流数据 SQL --------------------

num_orders = 5000
for i in range(1, num_orders + 1):
    user_id = random.randint(1, num_users)
    shop_id = random.randint(1, num_shops)
    order_time_dt = random_timestamp("2025-03-10 00:00:00", "2025-03-25 23:59:59")
    order_time_str = order_time_dt.strftime('%Y-%m-%d %H:%M:%S')
    order_amount = 0.0
    payment_method = random.choice(["Alipay", "WeChat Pay", "Credit Card", "PayPal"])
    shipping_address = f"Address of user {user_id}"
    coupon_code = None if random.random() > 0.7 else f"COUPON_{random.randint(100, 999)}"
    order_status = random.choice(["Pending", "Processing", "Shipped", "Delivered"])
    promotion_id = None
    # 注意：这里不再使用 spark.table 来获取促销活动信息，简化处理
    if random.random() < 0.5:
        promotion_id = random.randint(1, len(promotions_data)) # 随机选择一个促销活动 ID，不保证完全有效
    promotion_id_str = 'NULL' if promotion_id is None else str(promotion_id)
    coupon_code_str = 'NULL' if coupon_code is None else f"'{coupon_code}'"

    sql = f"""
        INSERT INTO {ORDERS_TABLE} (order_id, user_id, shop_id, order_time, order_amount, payment_method, shipping_address, promotion_id, coupon_code, order_status, shipping_id)
        VALUES ({i}, {user_id}, {shop_id}, '{order_time_str}', {order_amount}, '{payment_method}', '{shipping_address}', {promotion_id_str}, {coupon_code_str}, '{order_status}', NULL)
    """
    sql_statements.append(sql)

    num_items = random.randint(1, 3)
    for j in range(num_items):
        product_id = random.randint(1, num_products)
        quantity = random.randint(1, 5)
        cost_price = product_costs.get(product_id, 50.0) # 从字典获取成本价，默认 50.0
        unit_price = cost_price * random.uniform(1.2, 3.0)
        discount_rate = 1.0 - random.uniform(0, 0.2) if random.random() < 0.3 else 1.0
        item_amount = quantity * unit_price * discount_rate
        sql = f"""
            INSERT INTO {ORDER_ITEMS_TABLE} (item_id, order_id, product_id, quantity, unit_price, discount_rate)
            VALUES ({i * 10 + j}, {i}, {product_id}, {quantity}, {round(unit_price, 2)}, {round(discount_rate, 2)})
        """
        sql_statements.append(sql)
        order_amount += item_amount

    sql = f"UPDATE {ORDERS_TABLE} SET order_amount = {round(order_amount, 2)} WHERE order_id = {i}"
    sql_statements.append(sql)

    if order_status in ["Shipped", "Delivered"]:
        shipping_id = i
        shipping_carrier = random.choice(["DHL", "FedEx", "UPS", "SF Express"])
        tracking_number = f"{shipping_carrier}{random.randint(1000000000, 9999999999)}"
        dispatch_time_dt = order_time_dt + timedelta(hours=random.randint(1, 24))
        dispatch_time = dispatch_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        estimated_arrival_dt = order_time_dt + timedelta(days=random.randint(2, 7))
        estimated_arrival = estimated_arrival_dt.strftime('%Y-%m-%d %H:%M:%S')
        actual_arrival = None
        actual_arrival_str = 'NULL'
        if order_status == "Delivered":
            actual_arrival_dt = order_time_dt + timedelta(days=random.randint(2, 7), hours=random.randint(-12, 24))
            actual_arrival = actual_arrival_dt
            actual_arrival_str = f"'{actual_arrival.strftime('%Y-%m-%d %H:%M:%S')}'"
        shipping_cost = round(random.uniform(5, 30), 2)
        sql = f"""
            INSERT INTO {SHIPPING_INFO_TABLE} (shipping_id, order_id, shipping_carrier, tracking_number, dispatch_time, estimated_arrival, actual_arrival, shipping_cost)
            VALUES ({shipping_id}, {i}, '{shipping_carrier}', '{tracking_number}', '{dispatch_time}', '{estimated_arrival}', {actual_arrival_str}, {shipping_cost})
        """
        sql_statements.append(sql)
        sql = f"UPDATE {ORDERS_TABLE} SET shipping_id = {shipping_id} WHERE order_id = {i}"
        sql_statements.append(sql)

# -------------------- 执行 SQL 语句 --------------------
for sql in sql_statements:
    print(sql)
    spark.sql(sql)

spark.stop()