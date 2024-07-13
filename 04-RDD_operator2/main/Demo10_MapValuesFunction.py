from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示mapValues算子
   SourceFile  :	Demo10_MapValuesFunction
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
rdd_kv = sc.parallelize([('laoda',11),('laoer',22),('laosan',33),('laosi',44)], numSlices=2)

# 3.数据处理
result_rdd = rdd_kv.mapValues(lambda x:x + 10)

# 4.数据输出
print(result_rdd.collect())
print(result_rdd.glom().collect())

# 5.关闭SparkContext
sc.stop()
