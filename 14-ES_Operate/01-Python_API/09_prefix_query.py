from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义prefix查询条件，查询email字段中以"ali"为前缀的文档
query_body = {
    "query": {
        "prefix": {
            "email": "ali"  # 指定前缀查询条件
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)
