from elasticsearch import Elasticsearch

# 创建Elasticsearch客户端实例
es = Elasticsearch([{"host": "192.168.88.161", "port": 9200}])

# 定义regexp查询条件，查询email字段中符合正则表达式的文档
query_body = {
    "query": {
        "regexp": {
            "email": {
                "value": "a.*@example\\.com",  # 正则表达式匹配以a开头，后面跟任意字符，最终以@example.com结尾的邮箱
                "flags": "ALL"  # 使用所有可能的正则表达式语法
            }
        }
    }
}

# 执行查询
# 使用了正则表达式a.*@example\.com来匹配所有以a开头并以@example.com结尾的电子邮件地址
response = es.search(index="user", body=query_body)

# 输出查询结果
print(response)
