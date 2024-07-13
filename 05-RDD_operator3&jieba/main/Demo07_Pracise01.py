from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：试卷题1
   SourceFile  :	Demo06_Pracise01
   Author      :	81196
   Date	       :	2023/9/10
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
input_rdd = sc.textFile(get_absolute_path("../data/data.txt"))

# 3.数据处理
tuple_rdd = input_rdd.flatMap(lambda line: line.split(" "))
result_rdd = tuple_rdd.map(lambda x: (x.split("-")[0], x.split("-")[1]))
result_rdd2 = result_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)
# 4.数据输出
print(result_rdd.collect())
print(result_rdd2.collect())

# 5.关闭SparkContext
sc.stop()
