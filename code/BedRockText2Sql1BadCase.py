import boto3
import json

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2"  # Change to your region
)

# to english and use claude get
messagesToUseTool = """
你是一个跨境电商领域的数据洞察专家,你接受数据需求并理解,你需要思考需要哪些数据，为什么
然后直接输出一个规范的spark sql不要返回其他字符
数据需求为:##USER_INPUT
数据库表格数据DDL:CREATE TABLE testdb.commerce_shopping (
    user_id    STRING    COMMENT '用户ID（非真实ID），经抽样&字段脱敏处理后得到',
    item_id    STRING    COMMENT '商品ID（非真实ID），经抽样&字段脱敏处理后得到',
    item_category    STRING    COMMENT '商品类别ID（非真实ID），经抽样&字段脱敏处理后得到',
    behavior_type    STRING    COMMENT '用户对商品的行为类型,包括浏览、收藏、加购物车、购买，pv,fav,cart,buy)',
    behavior_time    STRING    COMMENT '行为时间,精确到小时级别'
)
示例查询 select count(user_id) from testdb.commerce_shopping where behavior_type = pv;
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



def chat(data_input, user_input, model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"):
    messages = [
        {
            "role": "user",
            "content": messageGetSignal.replace("##USER_INPUT", user_input).replace("##DATA_INPUT", json.dumps(data_input))
        }
    ]
    request_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
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
    max_loop = 0
    # Continue conversation until no more tool calls are needed
    while True:
        # Create the request payload
        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
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


def lambda_handler(event, context):
    user_input = event.get("user_input")
    model_id = event.get("model_id", "us.anthropic.claude-3-7-sonnet-20250219-v1:0")
    sql = get_data_use_tool(user_input, model_id)
    print("\nFinal response for query 1:" + sql)
    data_input = query_db_by_invoke_lambda(sql)
    print("\nFinal response for query 1:" + json.dumps(data_input))
    result = chat(data_input, user_input,model_id)
    print("\nFinal response for query 2:" + json.dumps(result))
    resultLog = {
        "statusCode": 200,
        "result": result,
        "data": data_input
    }
    invocation_id = event.get('invocation_id')
    write_result(resultLog, invocation_id)
    return resultLog

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
    event = {
        "user_input": query,
        "invocation_id": "1234567890"
    }
    result = lambda_handler(event, None)
