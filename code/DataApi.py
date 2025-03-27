# Define multiple tools
import boto3
import json


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
        from_data_api = main_query.get('from_data_api', '')
        if not from_data_api:
            return "ERROR: No from_data_api specified"

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
    # if tool_name == "executeSqlQuery":
    sql = json_to_sql(tool_params)
    # 以Error开头，表示出错
    if sql.startswith("ERROR"):
        return sql, ""
    sql_result = query_db_by_invoke_lambda(sql)
    return sql_result, sql


def lambda_handler(event, context):
    tool_name = event.get("tool_name")
    tool_params = event.get("tool_params")
    sql_result, sql = execute_tool(tool_name, tool_params)
    return {
        "statusCode": 200,
        "sql_result": sql_result,
        "sql": sql
    }
