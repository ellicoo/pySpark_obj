from pyspark import SparkContext, SparkConf
import os

from pyspark.sql import SparkSession

"""
-------------------------------------------------
   Description :	TODO：SparkSQL使用
   SourceFile  :	Demo02_SQLPractise
   Author      :	81196
   Date	       :	2023/9/12
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkContext
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# 2.数据输入
df = spark.createDataFrame([(1001, 'zhangsan', 19), (1002, 'lisi', 20), (1003, 'wangwu', 21)], ['id', 'name', 'age'])

# 3.数据处理
# 3.1 把数据注册成一张表(视图）
df.createOrReplaceTempView("people")
# 3.2 使用
result_df = spark.sql("select id,name,age from people")

# 4.数据输出
df.printSchema()
df.show()
result_df.show()

# 5.关闭SparkContext
spark.stop()
