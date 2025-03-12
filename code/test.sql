-- 数据来源：某电商网站用户行为数据，已脱敏
-- ddl
CREATE TABLE s3tablesbucket.testdb.commerce_shopping_big (
user_id    STRING    COMMENT '用户ID（非真实ID），经抽样&字段脱敏处理后得到',
item_id    STRING    COMMENT '商品ID（非真实ID），经抽样&字段脱敏处理后得到',
item_category    STRING    COMMENT '商品类别ID（非真实ID），经抽样&字段脱敏处理后得到',
behavior_type    STRING    COMMENT '用户对商品的行为类型,包括浏览、收藏、加购物车、购买，pv,fav,cart,buy)',
behavior_time    STRING    COMMENT '行为时间,精确到小时级别' ) USING iceberg

-- 用户行为漏斗分析
-- 总用户数(total_users)
-- 浏览过商品的用户数(users_with_views)
-- 收藏过商品的用户数(users_with_favorites)
-- 加入购物车的用户数(users_with_cart_adds)
-- 完成购买的用户数(users_with_purchases)
-- 浏览率(view_rate)：浏览用户占总用户百分比
-- 浏览到收藏转化率(view_to_favorite_rate)
-- 收藏到加购转化率(favorite_to_cart_rate)
-- 加购到购买转化率(cart_to_purchase_rate)
-- 整体转化率(overall_conversion_rate)：购买用户占总用户百分比
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

-- 商品关联推荐  查询实现了一个商品关联分析（关联规则挖掘），用于发现哪些商品经常被一起购买。
--商品对(item_a和item_b)
--商品对共同购买频次(pair_frequency)
--各自的购买频次(freq_a和freq_b)
--从item_a到item_b的置信度
--从item_b到item_a的置信度
--商品对的Jaccard相似度
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
-- 成对出现的商品类别(category_a和category_b)
-- 同时购买这两个类别商品的用户数量(common_users)
-- 同时购买两个类别的用户占category_a用户总数的百分比(percentage_of_category_a_users)
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
-- 商品类别(item_category)
-- 该类别的购买总次数(total_buy)
-- 购买率(buy_percent_rate)：购买次数占浏览次数的百分比
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