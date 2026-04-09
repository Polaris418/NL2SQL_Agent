from __future__ import annotations


FEW_SHOT_EXAMPLES: dict[str, list[dict[str, str]]] = {
    "postgresql": [
        {
            "question": "??30????????",
            "sql": "SELECT DATE(created_at) AS date, COUNT(*) AS order_count FROM orders WHERE created_at >= NOW() - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY date",
        },
        {
            "question": "?????????????10?",
            "sql": "SELECT user_id, SUM(total_amount) AS total_spent FROM orders GROUP BY user_id ORDER BY total_spent DESC LIMIT 10",
        },
    ],
    "mysql": [
        {
            "question": "??30????????",
            "sql": "SELECT DATE(created_at) AS date, COUNT(*) AS order_count FROM orders WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY DATE(created_at) ORDER BY date",
        },
        {
            "question": "?????????????10?",
            "sql": "SELECT user_id, SUM(total_amount) AS total_spent FROM orders GROUP BY user_id ORDER BY total_spent DESC LIMIT 10",
        },
        {
            "question": "????7??????",
            "sql": "SELECT COUNT(DISTINCT user_id) AS active_user_count FROM t_tool_usage WHERE used_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)",
        },
        {
            "question": "????7???????",
            "sql": "SELECT u.id, u.username, COUNT(*) AS usage_count FROM t_tool_usage tu JOIN t_user u ON tu.user_id = u.id WHERE tu.used_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) GROUP BY u.id, u.username ORDER BY usage_count DESC LIMIT 10",
        },
    ],
    "sqlite": [
        {
            "question": "??30????????",
            "sql": "SELECT DATE(created_at) AS date, COUNT(*) AS order_count FROM orders WHERE created_at >= datetime('now', '-30 day') GROUP BY DATE(created_at) ORDER BY date",
        },
        {
            "question": "?????????????10?",
            "sql": "SELECT user_id, SUM(total_amount) AS total_spent FROM orders GROUP BY user_id ORDER BY total_spent DESC LIMIT 10",
        },
    ],
}

EXAMPLES = {
    db_type: [f"Question: {item['question']}\nSQL: {item['sql']}" for item in items]
    for db_type, items in FEW_SHOT_EXAMPLES.items()
}


def format_few_shot_examples(db_type: str) -> str:
    examples = FEW_SHOT_EXAMPLES.get(db_type.lower(), [])
    if not examples:
        return "?"
    chunks = []
    for example in examples:
        chunks.append(f"??: {example['question']}\nSQL: {example['sql']}")
    return "\n\n".join(chunks)


def get_few_shot_examples(db_type: str) -> list[dict[str, str]]:
    return list(FEW_SHOT_EXAMPLES.get(db_type.lower(), []))
