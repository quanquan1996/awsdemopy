-- ddl
CREATE TABLE s3tablesbucket.testdb.commerce_shopping_big (
user_id    STRING    COMMENT '用户ID（非真实ID），经抽样&字段脱敏处理后得到',
item_id    STRING    COMMENT '商品ID（非真实ID），经抽样&字段脱敏处理后得到',
item_category    STRING    COMMENT '商品类别ID（非真实ID），经抽样&字段脱敏处理后得到',
behavior_type    STRING    COMMENT '用户对商品的行为类型,包括浏览、收藏、加购物车、购买，pv,fav,cart,buy)',
behavior_time    STRING    COMMENT '行为时间,精确到小时级别' ) USING iceberg

-- 用户行为漏斗分析
WITH user_behavior_counts AS (
    SELECT
        user_id,
        SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS view_count,
        SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS favorite_count,
        SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
        SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS purchase_count
    FROM testdb.commerce_shopping
    GROUP BY user_id
),
funnel_stages AS (
    SELECT
        COUNT(DISTINCT user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN view_count > 0 THEN user_id END) AS users_with_views,
        COUNT(DISTINCT CASE WHEN favorite_count > 0 THEN user_id END) AS users_with_favorites,
        COUNT(DISTINCT CASE WHEN cart_count > 0 THEN user_id END) AS users_with_cart_adds,
        COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN user_id END) AS users_with_purchases
    FROM user_behavior_counts
)
SELECT
    total_users,
    users_with_views,
    users_with_favorites,
    users_with_cart_adds,
    users_with_purchases,
    ROUND(100.0 * users_with_views / total_users, 2) AS view_rate,
    ROUND(100.0 * users_with_favorites / users_with_views, 2) AS view_to_favorite_rate,
    ROUND(100.0 * users_with_cart_adds / users_with_favorites, 2) AS favorite_to_cart_rate,
    ROUND(100.0 * users_with_purchases / users_with_cart_adds, 2) AS cart_to_purchase_rate,
    ROUND(100.0 * users_with_purchases / total_users, 2) AS overall_conversion_rate
FROM funnel_stages;

-- 商品关联推荐
WITH user_purchases AS (
    SELECT
        user_id,
        item_id
    FROM testdb.commerce_shopping
    WHERE behavior_type = 'buy'
    GROUP BY user_id, item_id
),
item_pairs AS (
    SELECT
        a.item_id AS item_a,
        b.item_id AS item_b
    FROM user_purchases a
    JOIN user_purchases b ON a.user_id = b.user_id AND a.item_id < b.item_id
),
item_pair_counts AS (
    SELECT
        item_a,
        item_b,
        COUNT(*) AS pair_frequency
    FROM item_pairs
    GROUP BY item_a, item_b
),
item_counts AS (
    SELECT
        item_id,
        COUNT(DISTINCT user_id) AS item_frequency
    FROM user_purchases
    GROUP BY item_id
),
association_rules AS (
    SELECT
        p.item_a,
        p.item_b,
        p.pair_frequency,
        a.item_frequency AS freq_a,
        b.item_frequency AS freq_b,
        p.pair_frequency / a.item_frequency AS confidence_a_to_b,
        p.pair_frequency / b.item_frequency AS confidence_b_to_a,
        p.pair_frequency / (a.item_frequency + b.item_frequency - p.pair_frequency) AS jaccard_similarity
    FROM item_pair_counts p
    JOIN item_counts a ON p.item_a = a.item_id
    JOIN item_counts b ON p.item_b = b.item_id
)
SELECT
    item_a,
    item_b,
    pair_frequency,
    freq_a,
    freq_b,
    ROUND(confidence_a_to_b, 4) AS confidence_a_to_b,
    ROUND(confidence_b_to_a, 4) AS confidence_b_to_a,
    ROUND(jaccard_similarity, 4) AS jaccard_similarity
FROM association_rules
WHERE pair_frequency >= 5
ORDER BY jaccard_similarity DESC
    LIMIT 20;

-- 商品类别交叉购买分析
WITH user_category_purchases AS (
    SELECT
        user_id,
        item_category
    FROM testdb.commerce_shopping
    WHERE behavior_type = 'buy'
    GROUP BY user_id, item_category
)
SELECT
    a.item_category AS category_a,
    b.item_category AS category_b,
    COUNT(DISTINCT a.user_id) AS common_users,
    COUNT(DISTINCT a.user_id) /
    (SELECT COUNT(DISTINCT user_id) FROM user_category_purchases WHERE item_category = a.item_category)::FLOAT * 100
        AS percentage_of_category_a_users
FROM user_category_purchases a
         JOIN user_category_purchases b ON a.user_id = b.user_id AND a.item_category < b.item_category
GROUP BY a.item_category, b.item_category
HAVING COUNT(DISTINCT a.user_id) >= 5
ORDER BY common_users DESC
    LIMIT 20;

-- 商品类别购买转换率
SELECT a.item_category AS item_category,
       b.total_buy,
       ROUND((b.total_buy / a.total_pv) * 100,2) AS buy_percent_rate
FROM
    (
        SELECT item_category,
               COUNT(*) as total_pv
        FROM testdb.commerce_shopping
        WHERE behavior_type = 'pv'
        GROUP BY item_category
    ) a
        INNER JOIN
    (
        SELECT item_category,
               COUNT(*) as total_buy
        FROM testdb.commerce_shopping
        WHERE behavior_type = 'buy'
        GROUP BY item_category
    ) b
    ON a.item_category = b.item_category
ORDER BY b.total_buy DESC
    LIMIT 20;