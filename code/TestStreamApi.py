import json
import time

def lambda_handler(event, context):
    # 设置响应头以支持流式传输
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Transfer-Encoding": "chunked"
        },
        "isBase64Encoded": False,
        "body": ""
    }

    # 创建一个生成器函数来流式传输响应
    def generate_stream():
        for i in range(10):
            yield f"data: {json.dumps({'message': f'This is chunk {i}', 'timestamp': time.time()})}\n\n"
            time.sleep(0.5)  # 模拟处理时间

    # 设置响应体为生成器
    response["body"] = generate_stream()

    return response
