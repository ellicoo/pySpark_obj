from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示Union和Distinct算子
   SourceFile  :	Demo02_Union&DistinctFunctions
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
list1 = [1,2,3,4,5,6,7,8]
list2 = [5,6,7,8,9,10]
input_rdd1 = sc.parallelize(list1)
input_rdd2 = sc.parallelize(list2)

# 3.数据处理
#3.1 使用union算子来合并RDD
result_rdd1 = input_rdd1.union(input_rdd2)
#3.2 使用distinct算子对合并后的数据进行去重
result_rdd2 = result_rdd1.distinct()


# 4.数据输出
#collect：会把结果收集到Driver内存中去
#foreach：它不会把结果返回给Driver，直接在Executor端打印结果
print("=========1.union操作=========")
print(result_rdd1.collect())
print("=========2.distinct操作=========")
print(result_rdd2.collect())

# 5.关闭SparkContext
sc.stop()
