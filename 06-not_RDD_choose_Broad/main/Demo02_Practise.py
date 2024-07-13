from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo02_Practise
   Author      :	81196
   Date	       :	2023/9/11
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

input_rdd2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
print(f"分区数为：{input_rdd2.getNumPartitions()}")
print(f"每个分区内的数据为：{input_rdd2.glom().collect()}")

# 5.关闭SparkContext
sc.stop()
