import json
import boto3

# 初始化Bedrock客户端
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2"  # 根据您的区域更改
)

# 定义工具
tools = [
    {
        "name": "executeSqlQuery",
        "description": "执行数据查询，获取用户购物行为相关的数据,字段说明user_id    STRING    COMMENT '用户ID（非真实ID），经抽样&字段脱敏处理后得到',item_id    STRING    COMMENT '商品ID（非真实ID），经抽样&字段脱敏处理后得到',item_category    STRING    COMMENT '商品类别ID（非真实ID），经抽样&字段脱敏处理后得到',behavior_type    STRING    COMMENT '用户对商品的行为类型,包括浏览、收藏、加购物车、购买,值范围：pv,fav,cart,buy)',behavior_time    STRING    COMMENT '行为时间,eg:1511544070'",
        "input_schema": {
            "type": "object",
            "properties": {
                "main_query": {
                    "type": "object",
                    "description": "主查询定义",
                    "properties": {
                        "select_fields": {
                            "type": "array",
                            "description": "要查询的字段列表",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                        "description": "字段名称"
                                    },
                                    "alias": {
                                        "type": "string",
                                        "description": "字段别名（可选）"
                                    },
                                    "aggregate": {
                                        "type": "string",
                                        "enum": ["COUNT", "SUM", "AVG", "MAX", "MIN", "NONE"],
                                        "description": "聚合函数（可选）"
                                    }
                                },
                                "required": ["field"]
                            }
                        },
                        "from_table": {
                            "type": "string",
                            "enum": ["testdb.commerce_shopping"],
                            "description": "查询的表名"
                        },
                        "where_conditions": {
                            "type": "array",
                            "description": "查询条件",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                        "description": "条件字段"
                                    },
                                    "operator": {
                                        "type": "string",
                                        "enum": ["=", "<>", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"],
                                        "description": "比较操作符"
                                    },
                                    "value": {
                                        "type": ["string", "number", "boolean", "null", "array"],
                                        "description": "比较值，对于IN/NOT IN操作符使用数组"
                                    },
                                    "logic": {
                                        "type": "string",
                                        "enum": ["AND", "OR"],
                                        "description": "与其他条件的逻辑关系"
                                    },
                                    "sub_query": {
                                        "type": "object",
                                        "description": "子查询（当操作符为IN/NOT IN等时可用）",
                                        "$ref": "#/properties/main_query"
                                    }
                                },
                                "required": ["field", "operator"]
                            }
                        },
                        "group_by": {
                            "type": "array",
                            "description": "分组字段",
                            "items": {
                                "type": "string",
                                "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"]
                            }
                        },
                        "having_conditions": {
                            "type": "array",
                            "description": "HAVING条件",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                        "description": "条件字段"
                                    },
                                    "aggregate": {
                                        "type": "string",
                                        "enum": ["COUNT", "SUM", "AVG", "MAX", "MIN", "NONE"],
                                        "description": "聚合函数"
                                    },
                                    "operator": {
                                        "type": "string",
                                        "enum": ["=", "<>", ">", "<", ">=", "<="],
                                        "description": "比较操作符"
                                    },
                                    "value": {
                                        "type": ["string", "number", "boolean", "null"],
                                        "description": "比较值"
                                    },
                                    "logic": {
                                        "type": "string",
                                        "enum": ["AND", "OR"],
                                        "description": "与其他条件的逻辑关系"
                                    }
                                },
                                "required": ["field", "operator", "value"]
                            }
                        },
                        "order_by": {
                            "type": "array",
                            "description": "排序规则",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                        "description": "排序字段"
                                    },
                                    "direction": {
                                        "type": "string",
                                        "enum": ["ASC", "DESC"],
                                        "description": "排序方向"
                                    }
                                },
                                "required": ["field", "direction"]
                            }
                        },
                        "limit": {
                            "type": "integer",
                            "description": "限制返回的记录数量"
                        },
                        "offset": {
                            "type": "integer",
                            "description": "跳过的记录数量"
                        },
                        "joins": {
                            "type": "array",
                            "description": "表连接定义（由于只有一个表，此功能可能不适用）",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["INNER", "LEFT", "RIGHT", "FULL"],
                                        "description": "连接类型"
                                    },
                                    "table": {
                                        "type": "string",
                                        "enum": ["commerce_shopping"],
                                        "description": "连接的表名"
                                    },
                                    "alias": {
                                        "type": "string",
                                        "description": "表别名"
                                    },
                                    "on_conditions": {
                                        "type": "array",
                                        "description": "连接条件",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "left_field": {
                                                    "type": "string",
                                                    "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                                    "description": "左表字段"
                                                },
                                                "operator": {
                                                    "type": "string",
                                                    "enum": ["=", "<>", ">", "<", ">=", "<="],
                                                    "description": "比较操作符"
                                                },
                                                "right_field": {
                                                    "type": "string",
                                                    "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"],
                                                    "description": "右表字段"
                                                },
                                                "logic": {
                                                    "type": "string",
                                                    "enum": ["AND", "OR"],
                                                    "description": "与其他条件的逻辑关系"
                                                }
                                            },
                                            "required": ["left_field", "operator", "right_field"]
                                        }
                                    }
                                },
                                "required": ["type", "table", "on_conditions"]
                            }
                        }
                    },
                    "required": ["select_fields", "from_table"]
                }
            },
            "required": ["main_query"]
        }
    }

]


# 处理json2sql
def json_to_sql(json_input):
    """
    将JSON格式的查询定义转换为标准SQL语句

    Args:
        json_input (dict): 符合schema的JSON对象

    Returns:
        str: 生成的SQL查询语句
    """
    try:
        # 提取主查询部分
        main_query = json_input.get('main_query', {})
        if not main_query:
            return "ERROR: Missing main_query in input"

        # 处理SELECT部分
        select_fields = main_query.get('select_fields', [])
        if not select_fields:
            return "ERROR: No select fields specified"

        select_clause = []
        for field in select_fields:
            field_name = field.get('field')
            alias = field.get('alias', '')
            aggregate = field.get('aggregate', 'NONE')

            if aggregate and aggregate != 'NONE':
                formatted_field = f"{aggregate}({field_name})"
            else:
                formatted_field = field_name

            if alias:
                formatted_field += f" AS {alias}"

            select_clause.append(formatted_field)

        # 处理FROM部分
        from_table = main_query.get('from_table', '')
        if not from_table:
            return "ERROR: No from_table specified"

        # 处理JOIN部分
        joins = main_query.get('joins', [])
        join_clause = []
        for join in joins:
            join_type = join.get('type', 'INNER')
            join_table = join.get('table', '')
            join_alias = join.get('alias', '')
            on_conditions = join.get('on_conditions', [])

            if not join_table or not on_conditions:
                continue

            join_str = f"{join_type} JOIN {join_table}"
            if join_alias:
                join_str += f" AS {join_alias}"

            join_str += " ON "
            on_parts = []

            for i, condition in enumerate(on_conditions):
                left = condition.get('left_field', '')
                op = condition.get('operator', '=')
                right = condition.get('right_field', '')
                logic = condition.get('logic', 'AND')

                if not left or not right:
                    continue

                condition_str = f"{left} {op} {right}"

                if i > 0:
                    condition_str = f"{logic} {condition_str}"

                on_parts.append(condition_str)

            join_str += " ".join(on_parts)
            join_clause.append(join_str)

        # 处理WHERE部分
        where_conditions = main_query.get('where_conditions', [])
        where_clause = []

        for i, condition in enumerate(where_conditions):
            field = condition.get('field', '')
            op = condition.get('operator', '=')
            value = condition.get('value')
            logic = condition.get('logic', 'AND')

            if not field or op is None:
                continue

            # 处理不同类型的操作符
            if op in ('IS NULL', 'IS NOT NULL'):
                condition_str = f"{field} {op}"
            elif op in ('IN', 'NOT IN'):
                if isinstance(value, list):
                    formatted_values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
                    condition_str = f"{field} {op} ({formatted_values})"
                else:
                    condition_str = f"{field} {op} ({value})"
            else:
                if isinstance(value, str):
                    condition_str = f"{field} {op} '{value}'"
                elif value is None:
                    condition_str = f"{field} IS NULL" if op == '=' else f"{field} IS NOT NULL"
                else:
                    condition_str = f"{field} {op} {value}"

            if i > 0:
                condition_str = f"{logic} {condition_str}"

            where_clause.append(condition_str)

        # 处理GROUP BY部分
        group_by = main_query.get('group_by', [])
        group_clause = ", ".join(group_by) if group_by else ""

        # 处理HAVING部分
        having_conditions = main_query.get('having_conditions', [])
        having_clause = []

        for i, condition in enumerate(having_conditions):
            field = condition.get('field', '')
            aggregate = condition.get('aggregate', '')
            op = condition.get('operator', '=')
            value = condition.get('value')
            logic = condition.get('logic', 'AND')

            if not field or not op:
                continue

            if aggregate and aggregate != 'NONE':
                field_str = f"{aggregate}({field})"
            else:
                field_str = field

            if isinstance(value, str):
                condition_str = f"{field_str} {op} '{value}'"
            elif value is None:
                condition_str = f"{field_str} IS NULL" if op == '=' else f"{field_str} IS NOT NULL"
            else:
                condition_str = f"{field_str} {op} {value}"

            if i > 0:
                condition_str = f"{logic} {condition_str}"

            having_clause.append(condition_str)

        # 处理ORDER BY部分
        order_by = main_query.get('order_by', [])
        order_clause = []

        for order in order_by:
            field = order.get('field', '')
            direction = order.get('direction', 'ASC')

            if field:
                order_clause.append(f"{field} {direction}")

        # 处理LIMIT和OFFSET
        limit = main_query.get('limit')
        offset = main_query.get('offset')

        # 构建SQL查询
        sql = f"SELECT {', '.join(select_clause)}\nFROM {from_table}"

        if join_clause:
            sql += f"\n{' '.join(join_clause)}"

        if where_clause:
            sql += f"\nWHERE {' '.join(where_clause)}"

        if group_clause:
            sql += f"\nGROUP BY {group_clause}"

        if having_clause:
            sql += f"\nHAVING {' '.join(having_clause)}"

        if order_clause:
            sql += f"\nORDER BY {', '.join(order_clause)}"

        if limit is not None:
            sql += f"\nLIMIT {limit}"

        if offset is not None:
            sql += f"\nOFFSET {offset}"

        return sql

    except Exception as e:
        return f"ERROR: Failed to generate SQL - {str(e)}"


# 处理模型响应中的工具调用
def process_tool_calls(response_body):
    input = response_body.get("input", "")
    return json_to_sql(input)


# 发送消息到模型并处理响应
def chat_with_model(messages, model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"):
    try:
        # 准备请求体
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "messages": messages,
            "tools": tools
        }

        # 调用Bedrock API
        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )

        # 解析响应
        response_body = json.loads(response.get('body').read())
        return response_body

    except Exception as e:
        print(f"调用模型时出错: {str(e)}")
        return None


# 主函数
def lambda_handler(event, context):
    context = event.get("context")
    # 初始化对话
    messages = [
        {"role": "user", "content": context}
    ]

    # 第一次调用模型
    print("发送用户请求到模型...")
    response = chat_with_model(messages)

    if not response:
        print("获取响应失败")
        return
    print(response)
    if response.get("stop_reason") == "tool_use":
        tool_results = []
        for message in response.get("content"):
            if message.get("type") == "tool_use":
                print("模型请求使用工具...")
                tool_call = {
                    "id": message.get("id"),
                    "name": message.get("name"),
                    "input": message.get("input")
                }
                # 假设process_tool_calls函数可以处理单个工具调用
                result = process_tool_calls(tool_call)
                tool_results.append(result)
        print(tool_results)
    else:
        print(response.get("content"))


# 测试代码
event = {
    "context": "帮我查看点击转化成购买的概率"
}
lambda_handler(event, None)
