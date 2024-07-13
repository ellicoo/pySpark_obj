from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示keys和values算子
   SourceFile  :	Demo09_KeysAndValuesFunction
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
#3.1 获取所有的key
keys_rdd = rdd_kv.keys()
#3.2 获取所有的value
values_rdd = rdd_kv.values()

# 4.数据输出
print("======1.keys======")
print(keys_rdd.collect())
print("======2.values======")
print(values_rdd.collect())

# 5.关闭SparkContext
sc.stop()
