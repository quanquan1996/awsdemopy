import boto3
import json

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2"  # Change to your region
)

# Define multiple tools
tools = [
    {
        "name": "QueryUserShoppingBehaviorData",
        "description": "Execute data queries to retrieve user shopping behavior data.\nPlease give English input.\n\n**Field descriptions:**\n\n* **user_id (STRING):** A unique identifier assigned to each user after sampling and field desensitization. This is not the user's real ID.\n* **item_id (STRING):** A unique identifier assigned to each product after sampling and field desensitization. This is not the product's real ID.\n* **item_category (STRING):** The identifier for the product category, obtained after sampling and field desensitization. This is not the real category ID.\n* **behavior_type (STRING):** The type of user interaction with a product. Possible values are:\n\t* `pv`: View\n\t* `fav`: Favorite\n\t* `cart`: Add to cart\n\t* `buy`: Purchase\n* **behavior_time (STRING):** The timestamp of the user's behavior, represented as a Unix timestamp (e.g., 1511544070).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query_description": {
                    "type": "string",
                    "description": "Query description ,Query What And Why,How to use these data"
                },
                "main_query": {
                    "type": "object",
                    "description": "Main query definition",
                    "properties": {
                        "query_fields": {
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
                        "from_data_api": {
                            "type": "string",
                            "enum": ["testdb.commerce_shopping"],
                            "description": "Table name to query"
                        },
                        "data_filter_conditions": {
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
                    "required": ["query_fields", "from_data_api"]
                }
            },
            "required": ["main_query"]
        }
    }

]
# to english and use claude get
messagesToUseTool = """
你是一个跨境电商领域的数据洞察专家,你接受数据需求并理解,你需要思考需要哪些数据，为什么
你需要从已有的tooluse里面使用合适的tool一轮或者多轮地多角度的去拿到相关数据，从而方便进行数据分析洞察
如果目前的工具里面的数据口径无法支撑这个需求，请不要勉强回答，直接返回缺少哪些数据和建议
数据需求为:##USER_INPUT
"""
messageFormatTable = """
帮我把上下文中所有工具历史查询的数据以表格的形式输出，并在表格前附带数据解释，对于每个工具查询结果，先简短描述数据内容，然后用表格展示
不需要过多的解释和分析
"""
messageGetSignal = """
你是一个跨境电商领域的数据洞察专家,你根据接收到的需求，根据已有数据来进行分析
给出专业的见解和建议
此外如果有什么有价值的探索，请给出探索的建议
如果当前对话里面的数据不完美或者无法支撑需求，请额外给出建议
回复的思路为 1.数据见解  2.探索建议 3.数据建议
****目前的数据为:##DATA_INPUT
****目前的需求为：##USER_INPUT
"""


# Mock tool execution function

# Function to handle conversation with Claude, including multiple tool calls
def execute_tool(tool_name, tool_params):
    # 创建 Lambda 客户端
    lambda_client = boto3.client('lambda', region_name='us-west-2')

    # 构建请求事件
    event = {
        "tool_name": tool_name,
        "tool_params": tool_params
    }

    # 将事件转换为JSON字符串
    payload = json.dumps(event)

    # 替换为您的Lambda函数名称
    function_name = "EcommerceDataApi"

    # 调用Lambda函数
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # 同步调用
        Payload=payload
    )

    # 处理响应
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)
    print(result)
    sql = result["sql"]
    sql_result = result["sql_result"]
    return sql_result, sql


def chat(data_input, user_input, model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"):
    messages = [
        {
            "role": "user",
            "content": messageGetSignal.replace("##USER_INPUT", user_input).replace("##DATA_INPUT", data_input)
        }
    ]
    request_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 102400,
        "messages": messages
    }

    # Invoke the model
    response = bedrock_runtime.invoke_model(
        modelId=model_id,
        body=json.dumps(request_payload)
    )

    # Parse the response
    response_body = json.loads(response.get("body").read())
    return response_body["content"][0]["text"]


def get_data_use_tool(user_input, model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"):
    messages = [
        {
            "role": "user",
            "content": messagesToUseTool.replace("##USER_INPUT", user_input)
        }
    ]
    data_source = []
    max_loop = 0
    # Continue conversation until no more tool calls are needed
    while True:
        # Create the request payload
        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 102400,
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
                    tool_result, sql = execute_tool(tool_name, tool_input)
                    data_source.append(
                        f"data source:{tool_name}\n,data search method:sql,\ndata search method payload:{sql}\n")
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
                max_loop = max_loop + 1
                if max_loop >= 2:
                    # 输出数据表格
                    messages.append({
                        "role": "user",
                        "content": messageFormatTable
                    })
                    request_payload = {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 102400,
                        "messages": messages,
                        "tools": tools
                    }
                    response = bedrock_runtime.invoke_model(
                        modelId=model_id,
                        body=json.dumps(request_payload)
                    )
                    response_body = json.loads(response.get("body").read())
                    return response_body["content"][0]["text"], data_source
                # Continue the loop to see if more tool calls are needed
            else:
                print("Tool use indicated but no tool_use content found")
                break
        else:
            # No more tool calls, return the final response
            return response_body["content"][0]["text"], data_source


def lambda_handler(event, context):
    user_input = event.get("user_input")
    model_id = event.get("model_id", "us.anthropic.claude-3-7-sonnet-20250219-v1:0")
    data_input,data_source = get_data_use_tool(user_input, model_id)
    print("\nFinal response for query 1:" + data_input)
    result = chat(data_input, user_input, model_id)
    print("\nFinal response for query 2:" + result)
    resultLog = {
        "statusCode": 200,
        "result": result,
        "data": data_input,
        "data_source": data_source
    }
    invocation_id = event.get('invocation_id')
    write_result(resultLog, invocation_id)
    return resultLog


def write_result(result, invocation_id):
    s3_client = boto3.client('s3')
    s3_key = f'lambda-status/{invocation_id}.json'
    s3_client.put_object(
        Bucket='lambda-result-qpj-west-2',
        Key=s3_key,
        Body=json.dumps(result)
    )


# Example usage
if __name__ == "__main__":
    # Example 1:
    query = "帮我看看点击购买转换率"
    # getdata
    result1, data_source = get_data_use_tool(query,'us.anthropic.claude-3-5-haiku-20241022-v1:0')
    print("\nFinal response for query 1:" + result1)
    print("\ndatasource response for query 1:" + json.dumps(data_source))
    result2 = chat(result1, query,"us.anthropic.claude-3-5-haiku-20241022-v1:0")
    print("\nFinal response for query 2:" + result2)
