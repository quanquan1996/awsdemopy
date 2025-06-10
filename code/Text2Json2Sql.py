# pip install boto3,jsonschema
from dataclasses import dataclass
from typing import List, Dict, Any
import boto3
import json
from jsonschema import validate, ValidationError
## 如果不用duckdb，这一步注释掉
import DuckDbS3table


@dataclass
class Config:
    # 大模型调用的方法名
    FUNCTION_NAME: str
    # 大模型调用的视图名
    VIEW_NAME: str
    # 方法描述（提示词）
    DESCRIPTION: str
    # 字段名称
    FIELD_NAMES: List[str]
    # 字段描述
    FIELD_DESC: str
    # 视图sql（不用于提示大模型）
    VIEW_SQL: str


configs = [
    Config(
        FUNCTION_NAME="user_behavior_data_api",
        VIEW_NAME="user_behavior_view",
        DESCRIPTION="Query user behavior data through JSON queries similar to SQL",
        FIELD_NAMES=["user_id", "item_id", "category_l2","category_l1", "behavior_type","behavior_time"],
        FIELD_DESC="all field is string",
        VIEW_SQL="select a.user_id as user_id,b.category_l2 as category_l2,b.category_l1 as category_l1,a.item_id as item_id,a.behavior_type as behavior_type,a.behavior_time as behavior_time from testtable.testdb.commerce_shopping a left join  testtable.testdb.commerce_item_category b on a.item_category = b.category_l3"
    ),
    Config(
        FUNCTION_NAME="transactions_invoices_join_api",
        VIEW_NAME="transactions_invoices_view",
        DESCRIPTION="Query joined transaction and invoice data through JSON queries similar to SQL",
        FIELD_NAMES=[
            "company_code",
            "voucher_number",
            "posting_date",
            "original_date",
            "doc_type",
            "source_doc_number",
            "account_code",
            "account_name",
            "debit_amount",
            "transaction_credit_amount",
            "invoice_number",
            "invoice_date",
            "invoice_amount",
            "customer_code"
        ],
        FIELD_DESC="""
    company_code: 字符串，公司代码，例如 'C001'
    voucher_number: 整数，凭证编号，例如 271, 278
    posting_date: 整数，凭证过账年月，格式为 YYYYMM，例如 '202109'
    original_date: 整数，原始单据年月，格式为 YYYYMM，例如 '202109'
    doc_type: 整数，单据类型，例如 13
    source_doc_number: 整数，源单据编号，例如 68, 75
    account_code: 字符串，会计科目代码，例如 '6001.01'
    account_name: 字符串，会计科目名称，例如 'purchased goods'
    debit_amount: 数字，借方金额，例如 0.00 (显示为 '-')
    transaction_credit_amount: 数字，贷方金额，例如 2017.31, 7847.53
    invoice_number: 整数，发票号，例如 68, 75
    invoice_date: 整数，发票年月，格式为 YYYYMM，例如 '202109'
    invoice_amount: 数字，发票金额，例如 2017.31, 7847.53
    customer_code: 字符串，客户代码，例如 'A00059', 'A00084'
    """,
        VIEW_SQL="""
    SELECT 
        t.company_code AS company_code,
        CAST(t.voucher_number AS INTEGER) AS voucher_number,
        CAST(REPLACE(t.voucher_posting_date,'/', '') AS INTEGER) AS posting_date,
        CAST(REPLACE(t.original_document_date,'/', '') AS INTEGER) AS original_date,
        CAST(t.document_type AS INTEGER) AS doc_type,
        CAST(t.source_document_number AS INTEGER) AS source_doc_number,
        t.account_code AS account_code,
        t.account_name AS account_name,
        CAST(REPLACE(t.debit_amount, ',', '') AS DECIMAL(10,2)) AS debit_amount,
        CAST(REPLACE(t.credit_amount, ',', '') AS DECIMAL(10,2)) AS transaction_credit_amount,
        CAST(i.invoice_number AS INTEGER) AS invoice_number,
        CAST(REPLACE(i.invoice_date,'/', '') AS INTEGER) AS invoice_date,
        CAST(REPLACE(i.amount, ',', '') AS DECIMAL(10,2)) AS invoice_amount,
        i.customer_code AS customer_code
    FROM 
        testtable.testdb.transactions t
    JOIN 
        testtable.testdb.invoices i
    ON 
        t.voucher_number = i.voucher_number
    """
    )
    ,
    # 更多配置...
]
VIEW_MAPPING = {config.VIEW_NAME: config.VIEW_SQL for config in configs}


def generate_tools_json_from_config(config: Config) -> Dict[str, Any]:
    return {
        "name": config.FUNCTION_NAME,
        "description": config.DESCRIPTION,
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
                                        "enum": config.FIELD_NAMES,
                                        "description": f"Field name,Field info:{config.FIELD_NAMES}"
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
                            "enum": [config.VIEW_NAME],
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
                                        "enum": config.FIELD_NAMES,
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
                                "enum": config.FIELD_NAMES
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
                                        "enum": config.FIELD_NAMES,
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
                                        "enum": config.FIELD_NAMES,
                                        "description": "Sorting field,When using GROUP BY, any column in the SELECT list or ORDER BY clause must either be included in the GROUP BY or be part of an aggregate function."
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
                        }
                    },
                    "required": ["query_fields", "from_data_api"]
                }
            },
            "required": ["main_query"]
        }
    }

tools = [generate_tools_json_from_config(config) for config in configs]
# 根据tools得到schema map（function_name->input_schema）
schema_map = {tool["name"]: tool["input_schema"] for tool in tools}

def text2json(prompt):
    while True:
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 102400,
            "messages": messages,
            "tools": tools
        }
        # Initialize Bedrock client
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name="us-west-2"  # Change to your region
        )
        # Invoke the model
        response = bedrock_runtime.invoke_model(
            modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
            body=json.dumps(request_payload)
        )
        response_body = json.loads(response.get("body").read())
        tool_use_items = [
            item for item in response_body.get("content", [])
            if item.get("type") == "tool_use"
        ]
        if tool_use_items:
            tool_use_inputs = [item.get("input") for item in tool_use_items]
            isJsonError = False
            for tool_use in tool_use_items:
                tool_name = tool_use.get("name")
                tool_input = tool_use.get("input", {})
                # 100%成功率的关键验证json，不通过就重试
                try:
                    validate(instance=tool_input, schema=schema_map[tool_name])
                    print("JSON数据验证通过")
                except ValidationError as e:
                    print(f"JSON数据验证失败: {e}")
                    isJsonError = True
            # 检查无误返回tool_input列表
            if not isJsonError:
                return tool_use_inputs
        else:
            return []
def json2sql(json_input):
    try:
        # 提取主查询部分
        main_query = json_input.get('main_query', {})
        if not main_query:
            return "ERROR: Missing main_query in input"

        # 处理SELECT部分
        query_fields = main_query.get('query_fields', [])
        if not query_fields:
            return "ERROR: No select fields specified"

        select_clause = []
        for field in query_fields:
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
        from_data_api = f"({VIEW_MAPPING.get(main_query.get('from_data_api', ''))})"
        if not from_data_api:
            return "ERROR: No from_data_api specified"
        # 处理WHERE部分
        data_filter_conditions = main_query.get('data_filter_conditions', [])
        where_clause = []

        for i, condition in enumerate(data_filter_conditions):
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
        sql = f"SELECT {', '.join(select_clause)}\nFROM {from_data_api}"

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

## 如果不用duckdb，这一方法注释掉
def query_sql(sql):
    return DuckDbS3table.handler({"sql": sql}, None)


## test
prompt = "查询科目代码”为6001开头的数据且贷方发生额大于50万的部分"
jsonArr = text2json(prompt)
print(jsonArr)
for json_input in jsonArr:
    sql = json2sql(json_input)
    print(sql)
    ## 如果不用duckdb，这一步注释掉
    print(query_sql(sql))
