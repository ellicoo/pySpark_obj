from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义wildcard查询条件，查询email字段中以"ali"开头的文档
query_body = {
    "query": {
        "wildcard": {
            "email": "ali*"  # 使用通配符*来进行模糊匹配,# ?代表一个字符，*代表0个或多个字符
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)

print("-----------------------------------------------")
# from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
# es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义wildcard查询条件，查询email字段中以"a"开头，后面跟着任意单个字符，再跟着"ice"的文档
query_body = {
    "query": {
        "wildcard": {
            "email": "a?ice"  # 使用通配符?来匹配任意单个字符
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)
