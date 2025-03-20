import json

def json_to_sql(query_params):
    """
    将JSON格式的查询参数转换为SQL查询语句

    Args:
        query_params: 包含SQL查询参数的字典

    Returns:
        str: SQL查询语句
    """
    main_query = query_params.get("main_query", {})

    # 处理SELECT子句
    select_clause = build_select_clause(main_query.get("select_fields", []))

    # 处理FROM子句
    from_table = main_query.get("from_table", "")
    from_clause = f"FROM {from_table}"

    # 处理JOIN子句
    join_clause = build_join_clause(main_query.get("joins", []))

    # 处理WHERE子句
    where_clause = build_where_clause(main_query.get("where_conditions", []))

    # 处理GROUP BY子句
    group_by = main_query.get("group_by", [])
    group_by_clause = f"GROUP BY {', '.join(group_by)}" if group_by else ""

    # 处理HAVING子句
    having_clause = build_having_clause(main_query.get("having_conditions", []))

    # 处理ORDER BY子句
    order_by_clause = build_order_by_clause(main_query.get("order_by", []))

    # 处理LIMIT和OFFSET子句
    limit = main_query.get("limit")
    offset = main_query.get("offset")
    limit_clause = f"LIMIT {limit}" if limit is not None else ""
    offset_clause = f"OFFSET {offset}" if offset is not None else ""

    # 组合SQL语句
    sql_parts = [
        select_clause,
        from_clause,
        join_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
        limit_clause,
        offset_clause
    ]

    # 过滤掉空字符串，并用空格连接
    sql = " ".join(filter(bool, sql_parts))

    return sql

def build_select_clause(select_fields):
    """构建SELECT子句"""
    if not select_fields:
        return "SELECT *"

    field_expressions = []
    for field_info in select_fields:
        field = field_info.get("field")
        alias = field_info.get("alias", "")
        aggregate = field_info.get("aggregate", "NONE")

        if aggregate and aggregate != "NONE":
            expression = f"{aggregate}({field})"
        else:
            expression = field

        if alias:
            expression = f"{expression} AS {alias}"

        field_expressions.append(expression)

    return f"SELECT {', '.join(field_expressions)}"

def build_where_clause(where_conditions):
    """构建WHERE子句"""
    if not where_conditions:
        return ""

    conditions = []
    for i, condition in enumerate(where_conditions):
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")
        logic = condition.get("logic", "AND") if i > 0 else ""

        # 处理不同类型的操作符
        if operator in ("IS NULL", "IS NOT NULL"):
            condition_str = f"{field} {operator}"
        elif operator in ("IN", "NOT IN"):
            if isinstance(value, list):
                formatted_values = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in value)
                condition_str = f"{field} {operator} ({formatted_values})"
            else:
                # 处理子查询，这里简化处理
                condition_str = f"{field} {operator} ({value})"
        else:
            if isinstance(value, str):
                condition_str = f"{field} {operator} '{value}'"
            else:
                condition_str = f"{field} {operator} {value}"

        if logic and i > 0:
            conditions.append(f"{logic} {condition_str}")
        else:
            conditions.append(condition_str)

    return f"WHERE {' '.join(conditions)}"

def build_having_clause(having_conditions):
    """构建HAVING子句"""
    if not having_conditions:
        return ""

    conditions = []
    for i, condition in enumerate(having_conditions):
        field = condition.get("field")
        aggregate = condition.get("aggregate", "NONE")
        operator = condition.get("operator")
        value = condition.get("value")
        logic = condition.get("logic", "AND") if i > 0 else ""

        if aggregate and aggregate != "NONE":
            field_expr = f"{aggregate}({field})"
        else:
            field_expr = field

        if isinstance(value, str):
            condition_str = f"{field_expr} {operator} '{value}'"
        else:
            condition_str = f"{field_expr} {operator} {value}"

        if logic and i > 0:
            conditions.append(f"{logic} {condition_str}")
        else:
            conditions.append(condition_str)

    return f"HAVING {' '.join(conditions)}"

def build_order_by_clause(order_by):
    """构建ORDER BY子句"""
    if not order_by:
        return ""

    order_expressions = []
    for order_info in order_by:
        field = order_info.get("field")
        direction = order_info.get("direction", "ASC")
        order_expressions.append(f"{field} {direction}")

    return f"ORDER BY {', '.join(order_expressions)}"

def build_join_clause(joins):
    """构建JOIN子句"""
    if not joins:
        return ""

    join_expressions = []
    for join_info in joins:
        join_type = join_info.get("type", "INNER")
        table = join_info.get("table")
        alias = join_info.get("alias", "")
        on_conditions = join_info.get("on_conditions", [])

        table_expr = f"{table} {alias}" if alias else table
        join_expr = f"{join_type} JOIN {table_expr}"

        if on_conditions:
            on_parts = []
            for i, on_condition in enumerate(on_conditions):
                left = on_condition.get("left_field")
                operator = on_condition.get("operator")
                right = on_condition.get("right_field")
                logic = on_condition.get("logic", "AND") if i > 0 else ""

                on_part = f"{left} {operator} {right}"
                if logic and i > 0:
                    on_parts.append(f"{logic} {on_part}")
                else:
                    on_parts.append(on_part)

            join_expr = f"{join_expr} ON {' '.join(on_parts)}"

        join_expressions.append(join_expr)

    return " ".join(join_expressions)

def execute_sql_query(query_params):
    """
    执行SQL查询并返回结果

    Args:
        query_params: 包含SQL查询参数的字典

    Returns:
        dict: 包含查询结果的字典
    """
    # 将JSON参数转换为SQL查询
    sql_query = json_to_sql(query_params)

    # 在实际应用中，这里会执行SQL查询
    # 这里只是返回SQL查询字符串作为示例

    return {
        "name": "execute_sql_query",
        "parameters": query_params,
        "sql_query": sql_query
    }

# 示例用法
if __name__ == "__main__":
    sample_params = {
        "main_query": {
            "select_fields": [
                {
                    "field": "user_id"
                },
                {
                    "field": "behavior_type",
                    "alias": "action"
                },
                {
                    "field": "item_id",
                    "aggregate": "COUNT",
                    "alias": "item_count"
                }
            ],
            "from_table": "testdb.commerce_shopping",
            "where_conditions": [
                {
                    "field": "behavior_type",
                    "operator": "IN",
                    "value": ["purchase", "view"],
                    "logic": "AND"
                },
                {
                    "field": "behavior_time",
                    "operator": ">=",
                    "value": "2023-01-01"
                }
            ],
            "group_by": ["user_id", "behavior_type"],
            "having_conditions": [
                {
                    "field": "item_id",
                    "aggregate": "COUNT",
                    "operator": ">",
                    "value": 5
                }
            ],
            "order_by": [
                {
                    "field": "user_id",
                    "direction": "ASC"
                }
            ],
            "limit": 100
        }
    }

    result = execute_sql_query(sample_params)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("\nGenerated SQL Query:")
    print(result["sql_query"])
