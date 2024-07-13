from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义查询条件
query_body = {
    "query": {
        "bool": {
            "must": [
                {"match": {"name": "赵云"}}
            ],
            "filter": [
                {"range": {"age": {"gte": 20, "lte": 30}}}
            ]
        }
    }
}

# 执行查询
response = es.search(index="es_python", body=query_body)

# 输出查询结果
print(response)


print("---------------------------------------------------")
# from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
# es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义查询条件和过滤字段
query_body = {
    "query": {
        "match_all": {}  # 这里可以替换为你的具体查询条件
    },
    "_source": ["name", "age", "sex"] ,# 指定最终显示的字段
    'size': 20  # 不指定默认是10，最大值不超过10000（可以修改，但是同时会增加数据库压力）
}

# 执行查询
response = es.search(index="es_python", body=query_body)

# 输出查询结果中的指定字段
for hit in response['hits']['hits']:
    print(hit['_source'])

"""
match_all查询会返回所有文档，但是由于我们使用了_source参数，所以最终只会显示每个文档的name、age和sex字段。
你可以根据需要调整查询条件和_source中的字段


在Elasticsearch中，hits是一个JSON对象，它包含了查询结果的相关信息。当你执行一个搜索查询时，Elasticsearch会返回一个包含多个部分的响应体，
其中hits部分包含了匹配查询条件的文档。

具体来说，hits对象包含两个主要的字段：

total：表示匹配查询条件的文档总数。
hits：是一个数组，包含了每个匹配的文档的详细信息。每个数组元素都是一个包含了文档_source（文档的原始数据）、_id（文档的ID）、
_index（文档所在的索引）等信息的对象。
这里是一个简单的例子，展示了一个包含hits的Elasticsearch响应体的结构：

{
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "hits": [
      {
        "_index": "es_python",
        "_type": "_doc",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "name": "赵云",
          "age": 25,
          "sex": "male",
          // 其他字段...
        }
      }
      // 更多匹配的文档...
    ]
  }
}
在这个例子中，total字段告诉我们有一个文档匹配了查询条件。hits数组包含了这个文档的详细信息，
例如它的索引、类型、ID、评分（_score，表示匹配的相关性）和源数据（_source，即文档的内容）。
"""