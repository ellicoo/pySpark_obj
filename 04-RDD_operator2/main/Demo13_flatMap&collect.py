from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：测试模板是否可用
   SourceFile  :	Demo04_TestTemplate
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
print('------------------------------------')

# flapmap
# 创建一个包含字符串的RDD
data = ["Hello World", "I am learning Spark", "Spark is fun"]
rdd1 = sc.parallelize(data)  # 单机（本地）对象--转-- 分布式的带有分区的对象,不给分区数时，默认根据电脑CPU核心数来决定分区数
rdd2 = sc.parallelize(data, 3)  # 单机（本地）对象--转-- 分布式的带有分区的对象

# 使用flatMap将每个字符串拆分成单词，并转换为小写
# flatMap函数返回的是一个扁平化后的结果，而不是包含嵌套列表的结果
flatMap_rdd = rdd2.flatMap(lambda line: line.lower().split(" "))

# 打印结果
print(flatMap_rdd.collect())
print(f'parallelize(data) 不给分区数时，默认分区数量为：{rdd1.getNumPartitions()}')
print(f'parallelize(data,3) 给分区数时，默认分区数量为：{rdd2.getNumPartitions()}')

print('---------------------------------------')

# 返回值类型：
#
# map 返回一个与原始 RDD 大小相同的 RDD，其中每个元素都经过转换。
# flatMap 返回一个新的 RDD，其大小可以与原始 RDD 不同。每个输入元素可以映射到零个、一个或多个输出元素，因此可以产生更多的输出元素。
# 嵌套结构：
#
# map 通常用于一对一的转换，不会改变元素之间的嵌套结构。如果原始 RDD 是嵌套的，经过 map 转换后仍然是嵌套的。
# flatMap 可以用于一对多的转换，它将嵌套结构扁平化，将多个输出元素放置在一个新的 RDD 中，不保留嵌套结构。
nested_list = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
nested_list_rdd1 = sc.parallelize(nested_list)
nested_list_rdd2 = sc.parallelize(nested_list, 5)

# 使用flatMap将子列表展平--如果使用parallelize创建的并行集合，则flatmap主要的功能是去嵌套
# 如果使用textFile读出的rdd，flatmap主要的功能是去换行符号

flat_list = nested_list_rdd2.flatMap(lambda sublist: sublist)


# collect：

# collect--将rdd分布式对象中的每个分区数据，都发送到Driver中，形成一个python list 对象

# collect 分布式 --转-- 本地集合
# 相对于
# parallelize 本地或者单机对象 --转-- 分布式的带有分区的rdd集合对象

# collect之前时rdd分布式对象，collect之后变成本地的python集合对象了

print(flat_list.collect())
print(flat_list.glom().collect())
print(f'parallelize(nested_list) 不给分区数时，默认分区数量为：{nested_list_rdd1.getNumPartitions()}')
print(f'parallelize(nested_list, 5) 给分区数时，默认分区数量为：{nested_list_rdd2.getNumPartitions()}')
print('--------------------------------')

sc.stop()
