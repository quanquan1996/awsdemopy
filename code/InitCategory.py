import random

from DuckDbS3table import handler
from pyiceberg.catalog import load_catalog
import pyarrow as pa

# 创建二级分类到一级分类的映射
category_map = {
    # 服装与配饰
    "女装": "服装与配饰",
    "男装": "服装与配饰",
    "童装": "服装与配饰",
    "内衣": "服装与配饰",
    "鞋靴": "服装与配饰",
    "箱包": "服装与配饰",
    "珠宝首饰": "服装与配饰",
    "钟表": "服装与配饰",
    "配饰": "服装与配饰",

    # 美妆个护
    "护肤品": "美妆个护",
    "彩妆": "美妆个护",
    "香水": "美妆个护",
    "美发护发": "美妆个护",
    "个人护理": "美妆个护",
    "口腔护理": "美妆个护",
    "洗浴用品": "美妆个护",

    # 家居生活
    "家具": "家居生活",
    "家纺": "家居生活",
    "家装建材": "家居生活",
    "灯具": "家居生活",
    "厨具": "家居生活",
    "收纳整理": "家居生活",
    "家居饰品": "家居生活",
    "园艺用品": "家居生活",

    # 电子数码
    "手机": "电子数码",
    "电脑": "电子数码",
    "平板": "电子数码",
    "相机": "电子数码",
    "音响设备": "电子数码",
    "智能设备": "电子数码",
    "电子配件": "电子数码",

    # 家用电器
    "大型家电": "家用电器",
    "厨房电器": "家用电器",
    "生活电器": "家用电器",
    "个人护理电器": "家用电器",
    "影音电器": "家用电器",

    # 食品生鲜
    "零食小吃": "食品生鲜",
    "粮油调味": "食品生鲜",
    "酒水饮料": "食品生鲜",
    "生鲜水果": "食品生鲜",
    "肉禽蛋奶": "食品生鲜",
    "水产海鲜": "食品生鲜",
    "休闲食品": "食品生鲜",

    # 母婴用品
    "奶粉": "母婴用品",
    "尿裤": "母婴用品",
    "喂养用品": "母婴用品",
    "洗护用品": "母婴用品",
    "玩具": "母婴用品",
    "童车童床": "母婴用品",
    "孕妇用品": "母婴用品",

    # 运动户外
    "运动服装": "运动户外",
    "运动鞋": "运动户外",
    "健身器材": "运动户外",
    "户外装备": "运动户外",
    "游泳用品": "运动户外",
    "球类用品": "运动户外",
    "骑行装备": "运动户外",

    # 图书音像
    "纸质图书": "图书音像",
    "电子书": "图书音像",
    "音乐": "图书音像",
    "电影": "图书音像",
    "教育音像": "图书音像",

    # 汽车用品
    "汽车配件": "汽车用品",
    "汽车装饰": "汽车用品",
    "汽车电子": "汽车用品",
    "维修保养": "汽车用品",
    "车载用品": "汽车用品",

    # 医药保健
    "中西药品": "医药保健",
    "营养保健": "医药保健",
    "医疗器械": "医药保健",
    "隐形眼镜": "医药保健",
    "保健器材": "医药保健",

    # 宠物用品
    "宠物食品": "宠物用品",
    "宠物玩具": "宠物用品",
    "宠物医疗": "宠物用品",
    "宠物服饰": "宠物用品",
    "宠物清洁": "宠物用品",

    # 虚拟商品与服务
    "充值卡": "虚拟商品与服务",
    "游戏点卡": "虚拟商品与服务",
    "在线服务": "虚拟商品与服务",
    "旅游票务": "虚拟商品与服务",
    "培训课程": "虚拟商品与服务"
}

def get_random_category():
    """
    随机返回一个二级分类及其对应的一级分类

    返回:
        tuple: (一级分类, 二级分类)
    """
    # 随机选择一个二级分类
    category_l2 = random.choice(list(category_map.keys()))
    # 获取对应的一级分类
    category_l1 = category_map[category_l2]

    return (category_l1, category_l2)

# 使用示例
if __name__ == "__main__":
    # 获取5个随机分类示例
    for _ in range(5):
        category = get_random_category()
        print(f"一级分类: {category[0]}, 二级分类: {category[1]}")
    event = {
        "sql": "select distinct(item_category) from testtable.testdb.commerce_shopping;"
    }
    result = handler(event, None)
    # 格式{
    #   "statusCode": 200,
    #   "result": [
    #     [
    #       "4690421"
    #     ],
    #     [
    #       "1813868"
    #     ]]}
    caList = []
    for item in result['result']:
        category = get_random_category()
        category_l1 = category[0]
        category_l2 = category[1]
        category_l3 = item[0]
        # append json to caList
        caList.append({"category_l1": category_l1, "category_l2": category_l2, "category_l3": category_l3})
    rest_catalog = load_catalog(
        "catalog_name",
        **{
            "type": "rest",
            "warehouse": "arn:aws:s3tables:us-west-2:051826712157:bucket/testtable",
            "uri": "https://s3tables.us-west-2.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": "us-west-2",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        }
    )
    # 新建表
    table= rest_catalog.create_table(
        "testdb.commerce_item_category",
        schema=pa.schema(
            [
                ("category_l1", pa.string()),
                ("category_l2", pa.string()),
                ("category_l3", pa.string()),
            ]
        )
    )
    df = pa.Table.from_pylist(caList, schema=table.schema().as_arrow())
# #插入表
    table.append(df)