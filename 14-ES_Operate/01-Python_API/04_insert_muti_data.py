
# from elasticsearch import Elasticsearch

# es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 旧版本---不推荐，因为types已经弃用了
# doc = [
#     {'index':{'_index':'es_python','_type':'_doc','_id':1}},
#     {'name':'赵云','age':25,'sex':'male','birthday':'1995-01-01','salary':8000},
#     {'index':{'_index':'es_python','_type':'_doc','_id':2}},
#     {'name':'张飞','age':24,'sex':'male','birthday':'1996-01-01','salary':8000},
#     {'create':{'_index':'es_python','_type':'_doc','_id':3}},
#     {'name':'关羽','age':23,'sex':'male','birthday':'1996-01-01','salary':8000},
# ]
# es.bulk(index='es_python',doc_type='_doc',body=doc)

#新版本--推荐

from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义索引设置和映射
index_settings = {
    "settings": {
        "number_of_shards": 3,  # 设置主分片数量
        "number_of_replicas": 1  # 设置每个主分片的副本数量
    },
    "mappings": {
        "properties": {
            "name": {
                "type": "text"
            },
            "age": {
                "type": "integer"
            },
            "email": {
                "type": "keyword"  # 指定email字段为keyword类型
            }
            # 在这里添加其他字段和类型定义
        }
    }
}

# 创建索引
es.indices.create(index='user', body=index_settings)

# 插入测试数据
test_data = [
    {"name": "Alice", "age": 30, "email": "alice@example.com"},
    {"name": "Bob", "age": 25, "email": "bob@example.com"},
    {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
]

# 批量插入测试数据
actions = []
for i, data in enumerate(test_data):
    actions.append({"index": {"_index": "user", "_id": i+1}})
    actions.append(data)

# 执行批量插入操作
es.bulk(index='user', body=actions)
