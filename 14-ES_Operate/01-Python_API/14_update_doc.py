from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义查询条件，找到所有名为"Alice"的文档
query_body = {
    "query": {
        "match": {
            "name": "Alice"
        }
    }
}

# 执行查询
search_response = es.search(index="user", body=query_body)

# 检查是否找到了匹配的文档
if search_response['hits']['hits']:
    # 遍历所有找到的文档
    for doc in search_response['hits']['hits']:
        doc_id = doc['_id']  # 获取文档ID
        # 定义要更新的数据
        updated_data = {
            "doc": {
                "age": 32  # 假设我们要将所有名为"Alice"的文档的年龄更新为32
            }
        }
        # 使用update方法更新文档
        update_response = es.update(index="user", id=doc_id, body=updated_data)
        print(update_response)
else:
    print("没有找到名为'Alice'的文档。")
