from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：演示Spark读取小文件的案例
   SourceFile  :	Demo01_ReadSmallFile
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

# 有时候小文件被分配在多个分区有点浪费。此时尽量集中在一个分区进行处理。--引入wholeTextFiles
# 2.1 传统读取数据的算子
input_rdd1 = sc.textFile(get_absolute_path("../data/ratings100/"))
# 2.2 专门用来读取小文件的算子wholeTextFiles
input_rdd2 = sc.wholeTextFiles(get_absolute_path("../data/ratings100/"))

# 3.数据处理
print(f"textFile传统读取文件的方式，分区数为：{input_rdd1.getNumPartitions()}")
print(f"wholeTextFiles读取文件的方式，分区数为：{input_rdd2.getNumPartitions()}")

# 查看分区数量
print(f"textFiles读取的分区数：{input_rdd1.getNumPartitions()}")
print(f"wholeTextFiles读取的分区数：{input_rdd2.getNumPartitions()}")

# 4.数据输出


# 5.关闭SparkContext
sc.stop()
