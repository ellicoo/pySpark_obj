from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示常用触发类算子
   SourceFile  :	Demo01_ActionFunctions
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
lists = [1, 2, 3, 4, 5, 6, 7, 8, 9]
input_rdd = sc.parallelize(lists)

# 3.数据处理

# 4.数据输出
# 4.1 调用first算子
print("==========1.first算子===========")
print(f'first算子的结果是：{input_rdd.first()}')
print("==========2.take算子===========")
print(f'take算子的结果是：{input_rdd.take(5)}')
print("==========3.collect算子===========")
print(f'collect算子的结果是：{input_rdd.collect()}')
print("==========4.reduce算子===========")
print(f'reduce算子的结果是：{input_rdd.reduce(lambda x, y: x + y)}')
print("==========5.top算子===========")
print(f'top算子的结果是：{input_rdd.top(3)}')

# 5.关闭SparkContext
sc.stop()
