from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义索引设置
# 设置一个索引切分成3个部分，然后为每个切片准备一个副本，相当于总共有6个冗余索引，或者3个有效索引，heah插件加粗为主分片索引，细为分片副本
# heah插件颜色--黄色：主分片全部正常，副本分片不正常，一旦某个节点有问题就不安全，无法使用副本。绿色：主副本分片全部正常
# 可以在运行时改变副本分片数量，但不能改变主分片数量
index_settings = {
    "settings": {
        "number_of_shards": 3,  # 设置主分片数量
        "number_of_replicas": 1  # 设置每个主分片的副本数量
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
