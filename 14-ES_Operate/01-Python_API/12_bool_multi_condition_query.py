from elasticsearch import Elasticsearch
#
# # 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])
#
# # 定义bool查询条件，组合多个查询条件
query_body = {
    "query": {
        "bool": {
            "must": [
                {"match": {"name": "Alice"}}
            ],
            "should": [ #should为可能的条件
                {"prefix": {"email": "alice"}},
                {"term": {"age": 30}}
            ],
            "must_not": [
                {"range": {"age": {"gte": 40}}}  #它排除了年龄大于等于40岁的文档
            ],
            "filter": [
                {"term": {"email": "alice@example.com"}}
            ]
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)

print("---------------------------------------------")
"""
如果创建索引时候不指定字段类型，将无法通过上面的Elasticsearch查询得到结果。这可能是因为email字段在Elasticsearch中默认不是keyword类型，
而是被分析为text类型。在bool查询的filter部分，你应该使用email.keyword来确保精确匹配，而不是分析后的email字段。建议创建索引时确定字段类型
请尝试以下修改后的查询代码，没有指定类型时用。。。。
"""
from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义bool查询条件，组合多个查询条件
query_body = {
    "query": {
        "bool": {
            "must": [
                {"match": {"name": "Alice"}}
            ],
            "should": [
                {"prefix": {"email": "alice"}},
                {"term": {"age": 30}}
            ],
            "must_not": [
                {"range": {"age": {"gte": 40}}}
            ],
            "filter": [
                {"term": {"email.keyword": "alice@example.com"}}
            ]
        }
    }
}

# 执行查询
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)
