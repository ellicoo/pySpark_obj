from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# print(es.indices.exists(index='index_shopping'))
try:
    result = es.search(index='index_shopping')
    print(result)
except Exception as e:
    print(f"An error occurred: {e}")

# 删除索引
res = es.indices.delete(index='user')
print(res)

