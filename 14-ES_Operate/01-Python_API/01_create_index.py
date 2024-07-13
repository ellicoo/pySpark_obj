from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# print(es.indices.exists(index='index_shopping'))
try:
    result = es.search(index='index_shopping')
    print(result)
except Exception as e:
    print(f"An error occurred: {e}")

"""索引名index_shopping,ignore=400，表示忽视400这个错误，如果存在index_shopping时，会返回400"""
es.indices.create(index='index_shopping', ignore=400)
