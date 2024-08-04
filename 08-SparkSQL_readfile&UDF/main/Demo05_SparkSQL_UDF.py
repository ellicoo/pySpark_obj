from pyspark.sql import SparkSession

import os

from pyspark.sql.types import StringType
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：SparkSQL的UDF演示
   SourceFile  :	Demo05_SparkSQLUDF
   Author      :	81196
   Date	       :	2023/9/14
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkSession
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入
# text：value
# csv：指定分隔符(, )，可以自动判断为2列
input_df = spark.read.csv(path="../data/people.txt", schema="name string,age int", sep=", ")


# 3.数据处理 需求：给name列拼接一个字符串:_itcast
# 1.定义一个Python函数
# @F.udf(returnType=StringType())
def concat_name(name: str):
    return name + "_itcast"


# 2.把Python函数注册到Spark中
# 2.1 spark.udf.register注册
# DSL中使用的函数名 = register(在SQL中可以使用的函数名，自定义的Python函数，返回值类型)
concat_name_dsl = spark.udf.register(name="concat_name_sql", f=concat_name, returnType=StringType())
# result_df1 = input_df.select('name','age',concat_name_dsl('name').alias("concat_name"))
# 2.2 F.udf方式
# concat_name_dsl2 = F.udf(f=concat_name,returnType=StringType())
# result_df2 = input_df.select('name','age',concat_name_dsl2('name').alias("concat_name"))
# 2.3 使用@F.udf()注解（装饰器，语法糖）在Python的自定义函数上使用即可
# result_df3 = input_df.select('name','age',concat_name('name').alias("concat_name"))

print("========DSL测试=======")
# 4.数据输出
input_df.printSchema()
input_df.show()
# result_df1.show()
# result_df2.show()
# result_df3.show()
print("========SQL测试=======")
input_df.createOrReplaceTempView("t1")
spark.sql("select name,age,concat_name_sql(name) as concat_name from t1").show()

# 5.关闭SparkContext
spark.stop()
