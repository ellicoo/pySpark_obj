from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：单元测试第10套第1题
   SourceFile  :	Demo02_TestUnit02
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
conf = SparkConf().setMaster("local[2]").setAppName("AppName")
sc = SparkContext(conf=conf)

# 2.数据输入
input_rdd = sc.textFile("../data/word.txt", minPartitions=3)

# 3.数据处理
flat_map_rdd = input_rdd.flatMap(lambda line: line.split(" "))
re_rdd = flat_map_rdd.repartition(5)
from operator import add

result_rdd = re_rdd.map(lambda word: (word, 1)).foldByKey(10, add)

# 4.数据输出
print(result_rdd.collect())

# 5.关闭SparkContext
sc.stop()
