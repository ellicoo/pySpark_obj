from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义multi_match查询条件，查询name或email字段中包含"Alice"的文档
query_body = {
    "query": {
        "multi_match": {
            "query": "Alice",
            "fields": ["name", "email"]  # 指定要查询的多个字段
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)
