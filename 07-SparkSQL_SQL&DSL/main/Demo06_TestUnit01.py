from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：单元测试第9套第二题
   SourceFile  :	Demo06_TestUnit01
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
input_rdd = sc.textFile("../data/word.txt")

# 3.数据处理
result_rdd = input_rdd.flatMap(lambda line:line.split(" ")).filter(lambda word:word != "hadoop")
result_rdd2 = result_rdd.count()
result_rdd3 = input_rdd.flatMap(lambda line:line.split(" ")).sortBy(lambda x:x[0],ascending=True).coalesce(1)

# 4.数据输出
result_rdd.foreach(lambda x:print(x))
print(result_rdd2)
result_rdd3.foreach(lambda x:print(x))

# 5.关闭SparkContext
sc.stop()
