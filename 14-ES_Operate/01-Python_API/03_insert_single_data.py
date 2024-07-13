from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# print(es.indices.exists(index='index_shopping'))
try:
    result = es.search(index='index_salary')
    print(result)
except Exception as e:
    print(f"An error occurred: {e}")


"""索引名index_shopping,ignore=400，表示忽视400这个错误，如果存在index_shopping时，会返回400"""
print(es.indices.create(index='index_salary', ignore=400))

# 插入单条数据，如果索引库不存在则index就创建
body = {'name':'刘婵',"age":6,
		"sex":"male",'birthday':'1984-01-01',
		"salary":-12000}
es.index(index='es_python',doc_type='_doc',body=body)
