from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：map算子练习
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
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
input_rdd = sc.parallelize(['长津湖','流浪地球','长津湖之水门桥','长津湖之水门桥','流浪地球2'])

# 3.数据处理
#map算子，实现对数据的一对一转换处理
result_rdd = input_rdd.map(lambda  movie:movie + '<吴京主演>')

# 4.数据输出
print(input_rdd.collect())
result_rdd.foreach(lambda movie:print(movie))

# 5.关闭SparkContext
sc.stop()
