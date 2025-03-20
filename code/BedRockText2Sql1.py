import boto3
import json
import time

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2"  # Change to your region
)

# Define multiple tools
tools = [
    {
        "name": "executeSqlQuery",
        "description": "Execute data queries to retrieve user shopping behavior data. please give english input .Field descriptions: user_id STRING COMMENT 'User ID (not real ID), obtained after sampling & field desensitization', item_id STRING COMMENT 'Product ID (not real ID), obtained after sampling & field desensitization', item_category STRING COMMENT 'Product category ID (not real ID), obtained after sampling & field desensitization', behavior_type STRING COMMENT 'User behavior types for products, including view, favorite, add to cart, purchase, value range: pv,fav,cart,buy)', behavior_time STRING COMMENT 'Behavior time, eg:1511544070'",
        "input_schema": {
            "type": "object",
            "properties": {
                "main_query": {
                    "type": "object",
                    "description": "Main query definition",
                    "properties": {
                        "select_fields": {
                            "type": "array",
                            "description": "List of fields to query",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                 "behavior_time"],
                                        "description": "Field name"
                                    },
                                    "alias": {
                                        "type": "string",
                                        "description": "Field alias (optional,use english)"
                                    },
                                    "aggregate": {
                                        "type": "string",
                                        "enum": ["COUNT", "SUM", "AVG", "MAX", "MIN", "NONE"],
                                        "description": "Aggregate function (optional)"
                                    }
                                },
                                "required": ["field"]
                            }
                        },
                        "from_table": {
                            "type": "string",
                            "enum": ["testdb.commerce_shopping"],
                            "description": "Table name to query"
                        },
                        "where_conditions": {
                            "type": "array",
                            "description": "Query conditions",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                 "behavior_time"],
                                        "description": "Condition field"
                                    },
                                    "operator": {
                                        "type": "string",
                                        "enum": ["=", "<>", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL",
                                                 "IS NOT NULL"],
                                        "description": "Comparison operator"
                                    },
                                    "value": {
                                        "type": ["string", "number", "boolean", "null", "array"],
                                        "description": "Comparison value, use array for IN/NOT IN operators"
                                    },
                                    "logic": {
                                        "type": "string",
                                        "enum": ["AND", "OR"],
                                        "description": "Logical relationship with other conditions"
                                    },
                                    "sub_query": {
                                        "type": "object",
                                        "description": "Subquery (available when operator is IN/NOT IN, etc.)",
                                        "$ref": "#/properties/main_query"
                                    }
                                },
                                "required": ["field", "operator"]
                            }
                        },
                        "group_by": {
                            "type": "array",
                            "description": "Grouping fields",
                            "items": {
                                "type": "string",
                                "enum": ["user_id", "item_id", "item_category", "behavior_type", "behavior_time"]
                            }
                        },
                        "having_conditions": {
                            "type": "array",
                            "description": "HAVING conditions",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                 "behavior_time"],
                                        "description": "Condition field"
                                    },
                                    "aggregate": {
                                        "type": "string",
                                        "enum": ["COUNT", "SUM", "AVG", "MAX", "MIN", "NONE"],
                                        "description": "Aggregate function"
                                    },
                                    "operator": {
                                        "type": "string",
                                        "enum": ["=", "<>", ">", "<", ">=", "<="],
                                        "description": "Comparison operator"
                                    },
                                    "value": {
                                        "type": ["string", "number", "boolean", "null"],
                                        "description": "Comparison value"
                                    },
                                    "logic": {
                                        "type": "string",
                                        "enum": ["AND", "OR"],
                                        "description": "Logical relationship with other conditions"
                                    }
                                },
                                "required": ["field", "operator", "value"]
                            }
                        },
                        "order_by": {
                            "type": "array",
                            "description": "Sorting rules",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {
                                        "type": "string",
                                        "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                 "behavior_time"],
                                        "description": "Sorting field"
                                    },
                                    "direction": {
                                        "type": "string",
                                        "enum": ["ASC", "DESC"],
                                        "description": "Sorting direction"
                                    }
                                },
                                "required": ["field", "direction"]
                            }
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Limit the number of returned records"
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Number of records to skip"
                        },
                        "joins": {
                            "type": "array",
                            "description": "Table join definitions (may not be applicable as there is only one table)",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["INNER", "LEFT", "RIGHT", "FULL"],
                                        "description": "Join type"
                                    },
                                    "table": {
                                        "type": "string",
                                        "enum": ["commerce_shopping"],
                                        "description": "Table name to join"
                                    },
                                    "alias": {
                                        "type": "string",
                                        "description": "Table alias ,use english"
                                    },
                                    "on_conditions": {
                                        "type": "array",
                                        "description": "Join conditions",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "left_field": {
                                                    "type": "string",
                                                    "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                             "behavior_time"],
                                                    "description": "Left table field"
                                                },
                                                "operator": {
                                                    "type": "string",
                                                    "enum": ["=", "<>", ">", "<", ">=", "<="],
                                                    "description": "Comparison operator"
                                                },
                                                "right_field": {
                                                    "type": "string",
                                                    "enum": ["user_id", "item_id", "item_category", "behavior_type",
                                                             "behavior_time"],
                                                    "description": "Right table field"
                                                },
                                                "logic": {
                                                    "type": "string",
                                                    "enum": ["AND", "OR"],
                                                    "description": "Logical relationship with other conditions"
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
def query_db_by_invoke_lambda(sql):
    # 创建 Lambda 客户端
    lambda_client = boto3.client('lambda', region_name='us-west-2')

    # 构建请求事件
    event = {
        "region_name": "us-west-2",
        "catalog_id": "051826712157:s3tablescatalog/testtable",
        "work_group": "pyspark",
        "sql": sql
    }

    # 将事件转换为JSON字符串
    payload = json.dumps(event)

    # 替换为您的Lambda函数名称
    function_name = "AthenaSparkCalculation"

    # 调用Lambda函数
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # 同步调用
        Payload=payload
    )

    # 处理响应
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)
    return result

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
# Mock tool execution function
def execute_tool(tool_name, tool_params):
    if tool_name == "executeSqlQuery":
        sql =  json_to_sql(tool_params)
        sql_result = query_db_by_invoke_lambda(sql)
        result = f"query with sql:{sql},and result is:{sql_result}"
        return result

# Function to handle conversation with Claude, including multiple tool calls
def chat_with_claude(user_input, model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"):
    messages = [
        {
            "role": "user",
            "content": user_input
        }
    ]

    # Continue conversation until no more tool calls are needed
    while True:
        # Create the request payload
        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "messages": messages,
            "tools": tools
        }

        # Invoke the model
        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_payload)
        )

        # Parse the response
        response_body = json.loads(response.get("body").read())

        print(f"Response from Claude: {json.dumps(response_body, indent=2)}")

        # Check if the response contains a tool use request (based on stop_reason)
        if response_body.get("stop_reason") == "tool_use":
            # Find all tool_use content items
            tool_use_items = [
                item for item in response_body.get("content", [])
                if item.get("type") == "tool_use"
            ]

            if tool_use_items:
                print(f"Found {len(tool_use_items)} tool use requests")

                # Add the assistant's response with tool use to the conversation
                messages.append({
                    "role": "assistant",
                    "content": response_body.get("content")
                })

                # Process each tool use request and collect results
                tool_results = []
                for tool_use in tool_use_items:
                    tool_name = tool_use.get("name")
                    tool_input = tool_use.get("input", {})
                    tool_id = tool_use.get("id")

                    print(f"Executing tool: {tool_name} with input: {tool_input}")

                    # Execute the tool
                    tool_result = execute_tool(tool_name, tool_input)
                    print(f"Tool result: {tool_result}")
                    # Format the tool result
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": json.dumps(tool_result)
                    })

                # Add all tool results as a single user message
                messages.append({
                    "role": "user",
                    "content": tool_results
                })

                # Continue the loop to see if more tool calls are needed
            else:
                print("Tool use indicated but no tool_use content found")
                return response_body
        else:
            # No more tool calls, return the final response
            return response_body

# Example usage
if __name__ == "__main__":
    # Example 1: Single tool use
    query1 = "帮我看看点击购买转换率"
    result1 = chat_with_claude(query1)
    print("\nFinal response for query 1:")
    print(json.dumps(result1, indent=2))
