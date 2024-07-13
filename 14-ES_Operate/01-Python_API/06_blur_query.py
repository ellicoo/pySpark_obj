from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义模糊查询条件
query_body = {
    "query": {
        "match": {
            "name": "云"  # 这将匹配任何包含字符"云"的名字
        }
    },
    'size': 20  # 不指定默认是10，最大值不超过10000（可以修改，但是同时会增加数据库压力）
}

# 执行查询
response = es.search(index="es_python", body=query_body)

# 输出查询结果
print(response)
