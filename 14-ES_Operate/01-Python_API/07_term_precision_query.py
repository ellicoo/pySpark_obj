from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义term查询条件，精准匹配name字段为"Alice"的文档
query_body = {
    "query": {
        "term": {
            "name.keyword": "Alice"  # 使用term查询进行精准匹配
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)

print("-------------------------term多值查询------------------------")

# 定义terms查询条件，精准匹配name字段为"Alice"或"Bob"的文档
query_body = {
    "query": {
        "terms": {
            "name.keyword": ["Alice", "Bob"]  # 使用terms查询进行精准多值匹配
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)

