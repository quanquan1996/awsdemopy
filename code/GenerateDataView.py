import boto3
import json

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2"  # Change to your region
)
joinInputSchema = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["viewName", "baseTable", "joins", "columns"],
        "properties": {
            "viewName": {
                "type": "string",
                "description": "The name of the database view to be created",
                "pattern": "^[A-Za-z][A-Za-z0-9_]*$"
            },
            "schema": {
                "type": "string",
                "description": "The database schema where the view will be created",
                "default": "public"
            },
            "baseTable": {
                "type": "object",
                "description": "The primary table for the view",
                "required": ["name"],
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the base table"
                    },
                    "alias": {
                        "type": "string",
                        "description": "Optional alias for the base table"
                    }
                }
            },
            "joins": {
                "type": "array",
                "description": "Tables to join with the base table",
                "items": {
                    "type": "object",
                    "required": ["table", "type", "condition"],
                    "properties": {
                        "table": {
                            "type": "object",
                            "required": ["name"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Name of the table to join"
                                },
                                "alias": {
                                    "type": "string",
                                    "description": "Optional alias for the joined table"
                                }
                            }
                        },
                        "type": {
                            "type": "string",
                            "description": "Type of join operation",
                            "enum": ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"],
                            "default": "INNER JOIN"
                        },
                        "condition": {
                            "type": "string",
                            "description": "Join condition (e.g., 'table1.id = table2.id')"
                        }
                    }
                }
            },
            "columns": {
                "type": "array",
                "description": "Columns to be included in the view",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "required": ["source", "name"],
                    "properties": {
                        "source": {
                            "type": "string",
                            "description": "Source table or alias for this column"
                        },
                        "name": {
                            "type": "string",
                            "description": "Name of the column in the source table"
                        },
                        "alias": {
                            "type": "string",
                            "description": "Optional alias for the column in the view"
                        }
                    }
                }
            }
        }
    }
}
tools = [
    {
        "name": "generateDatabaseView",
        "description": "generate multiple database views with table joins",
        "input_schema": joinInputSchema
    }
]
prompt = """
    我有一组数据库表的DDL定义，我需要你帮我设计一些有用的数据库视图(views)，这些视图应该通过合理的表连接来提供业务价值。
    ## 数据库表定义
    ${DDL}
    ## 任务要求
    1. 分析上述DDL，理解表之间的关系
    2. 使用合理的tool来generate data view
    ## 注意
    1. 视图名称应该反映其业务用途
    """
def getMokeDataDDL():
    return """
    CREATE TABLE transactions (
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
    CREATE TABLE invoices (
    invoice_number STRING COMMENT '发票号',
    invoice_date STRING COMMENT '发票日期',
    amount STRING COMMENT '贷方',
    voucher_number STRING COMMENT '凭证编号',
    customer_code STRING COMMENT '客户编码'
)
    """
def genDataView():
    messages = [
        {
            "role": "user",
            "content": prompt.replace("${DDL}", getMokeDataDDL())
        }
    ]
    request_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 102400,
        "messages": messages,
        "tools": tools,
        "tool_choice":{
            "type":"tool",
            "name": "generateDatabaseView"
        }
    }

    # Invoke the model
    response = bedrock_runtime.invoke_model(
        modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        body=json.dumps(request_payload)
    )
    print(f"Response from Claude: {json.dumps(response, indent=2)}")