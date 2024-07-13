from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示分区操作算子
   SourceFile  :	Demo03_PartitionFunctions
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


# **数据**：有一个RDD读文件产生，有两个分区，每个分区有50万条数据
#
# **需求**：需要将RDD的数据进行一对一的处理转换，最后用将转换好的结果写入MySQL，怎么实现

# 每来一条数据都操作一次
# map和foreach都是对每个元素做一次操作，两个分区的数据操作就是进行100万次的调用操作。
# ---map和foreach都不适合，因为mysql比较频繁使用，等不得你，频繁读写数据库的操作代价昂贵


# 解决方法：不对每条数据进行操作。而选择每个分区做操作，实际上是语法糖--foreachPartition和mappatition
# 之所以做到就是因为定义的map、foreach函数是原子操作的，即只进行一步操作，而mappatition和foreachpartition实际上函数内部进行循环操作
# 但传入参数必须是一个可迭代的对象。

# **解决**：==基于分区来操作，每次读取一个分区代替每次读取一个元素，每个分区构建一个MySQL连接代替每个元素构建一个连接==

# 分区是一个可迭代对象。

# 缺点：如果分区内的数据量大会造成内存溢出，50万条可以，超过则通过reputations来增大分区处理

def f(iter):
    #  foreachpatition函数默认接受一个可以允许自定义的函数，且该函数的参数最好是一个可迭代对象
    print('f被调用..')
    for x in iter:
        print(x)

# f函数只被调用了2次--因为只有两个分区
sc.parallelize([1, 2, 3, 4, 5], numSlices=2).foreachPartition(f)

def f2(x):
    print('f2被调用..')
    print(x)
    # foreach函数默认接受一个可以允许自定义的函数，且该函数的参数必须是一个不可迭代对象，即非容器类型或者字符串类型
    # foreachPartition都可以
    # 下面时一个foreach错误示范
    # for xx in x:
    #     print(xx)

# f2函数被调用了5次
sc.parallelize([1, 2, 3, 4, 5], numSlices=2).foreach(f2)
print('-------------字符串内容的rdd的foreach--------------')
sc.parallelize(['a', 'b', 'c', 'd', 'f'], numSlices=2).foreach(f2)
print('-------------字符串内容的rdd的foreachPartition--------------')
sc.parallelize(['a', 'b', 'c', 'd', 'f'], numSlices=2).foreachPartition(f2)


# 5.关闭SparkContext
sc.stop()
