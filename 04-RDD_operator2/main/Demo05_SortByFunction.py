from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：演示sortBy算子
   SourceFile  :	Demo05_SortByFunction
   Author      :	81196
   Date	       :	2023/9/8
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
input_rdd = sc.textFile(get_absolute_path("../data/time_music_party.csv"))

# 3.数据处理
#sortBy算子，适用于非KV类型的RDD
result_rdd = input_rdd.sortBy(lambda line:line.split(",")[1], ascending=False)

# 4.数据输出
print("============1.分区数量==============")
print(result_rdd.getNumPartitions())
print("============2.分区内的元素==============")
result_rdd.glom().foreach(lambda x:print(x))
print("============3.所有元素==============")
result_rdd.foreach(lambda x:print(x))

# 5.关闭SparkContext
sc.stop()
