import re

import jieba
from pyspark import SparkContext, SparkConf, StorageLevel
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os
import time

"""
-------------------------------------------------
   Description :	TODO：演示搜狗数据分析案例
   SourceFile  :	Demo06_SogouExample
   Author      :	81196
   Date	       :	2023/9/10
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 程序开始时间：
startTime = time.time()

# 1.构建SparkContext
conf = SparkConf().setMaster("local[2]").setAppName("AppName")
sc = SparkContext(conf=conf)


# 这个路径可以是本地的文件系统，或者是分布式文件系统
sc.setCheckpointDir("path")
# 2.数据输入
input_rdd = sc.textFile(get_absolute_path("../data/Sogou.tsv"))


# 3.数据处理
# 3.1数据清洗处理
def map_line(line):
    # 使用空格切割-- \s是空格，\s+是任意数量的空格
    lines = re.split("\s+", line)
    # 返回切割后的数据，注意搜索词的中括号需要去掉
    return (lines[0], lines[1], lines[2][1:-1], lines[3], lines[4], lines[5])


filter_rdd = input_rdd.filter(lambda line: len(re.split("\s+", line)) == 6).map(lambda line: map_line(line))
filter_rdd.checkpoint()
# filter_rdd.cache()  # 要注释掉这个代码，因为：

# 对于RDD，缓存（cache）和持久化（persist）是用来将数据保存在内存中以提高性能的两种方式。
# 在您的代码中，您已经使用了 filter_rdd.cache() 将RDD缓存到内存中。同时，
# 您还尝试使用 filter_rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK) 来将RDD持久化到磁盘和内存中。

# 问题出在 checkpoint 和 persist 的交互上。当您使用 checkpoint 方法时，它会将RDD写入分布式文件系统中，
# 并且在内部标记为需要重新计算。这意味着，即使您使用了 cache() 来将RDD缓存到内存中，它也不会从内存中提取，而是从检查点重新计算。

# 因此，如果您已经使用 checkpoint 将RDD写入了检查点，那么在这种情况下，
# 使用 cache() 来将其缓存到内存中是多余的，因为RDD不会从内存中读取。这是为什么 filter_rdd.cache() 不再需要的原因。

# 如果您想要将RDD写入检查点并且确保从检查点中读取，然后对RDD进行持久化，
# 您可以根据我之前提到的方法进行操作。不过要注意，在持久化时，仍然可以选择存储级别，
# 例如 MEMORY_ONLY 或 MEMORY_AND_DISK，以确定数据的存储位置和性能。

persist_rdd = filter_rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
# StorageLevel.DISK_ONLY -- 仅磁盘且存1份
# StorageLevel.DISK_ONLY_2 -- 仅磁盘且存2份
# StorageLevel.DISK_ONLY_3 -- 仅磁盘且存3份
# StorageLevel.MEMORY_ONLY -- 仅内存且存1份
# StorageLevel.MEMORY_ONLY_2 -- 仅内存且存2份
# StorageLevel.MEMORY_AND_DISK -- 内存和磁盘
# StorageLevel.MEMORY_AND_DISK_2 = 内存和磁盘存两份
# StorageLevel.OFF_HEAP -- 堆（堆的大小通常比栈大得多，都在内存中）
# StorageLevel.MEMORY_AND_DISK_DESER --


# 工作中推荐使用下面两种--主要是第一种
# StorageLevel.MEMORY_AND_DISK
# StorageLevel.MEMORY_AND_DISK_2


# 3.2 需求一，统计热门搜索词Top10
result_rdd1 = (filter_rdd
               # 取关键词来进行切割
               .flatMap(lambda line: jieba.cut(line[2]))
               # 把关键词转换为（关键词，1）的形式
               .map(lambda word: (word, 1))
               # 对（关键词，1）的数据进行分组聚合
               .reduceByKey(lambda x, y: x + y)
               # 降低分区，准备全局排序
               .coalesce(1)
               # 使用关键词的次数进行降序排序
               .sortBy(lambda x: x[1], ascending=False)
               .take(10))

# 3.3需求二，统计所有用户所有搜索中最大搜索次数、最小搜索次数、平均搜索次数
result_rdd2 = (filter_rdd
               # 取数据中的(用户ID,搜索词)
               .map(lambda line: (line[1], line[2]))
               # 把数据转换为((用户ID,搜索词),1)
               .map(lambda line: (line, 1))
               # 分组统计
               .reduceByKey(lambda x, y: x + y)
               # 获取最终结果中的values值
               .values())

# 3.4 需求三，统计一天每小时搜索量并按照搜索量降序排序【统计每个小时数据量，按照数据量降序排序】
result_rdd3 = (filter_rdd
               # 取数据中的时间列，并且转换为小时，最终拼接成（小时，1）的形式
               .map(lambda line: (line[0][0:2], 1))
               # 对（小时，1）的数据进行分组聚合
               .reduceByKey(lambda x, y: x + y)
               # 降低分区，准备全局排序
               .coalesce(1)
               # 指定排序的规则为搜索量，倒序
               .sortBy(lambda x: x[1], ascending=False))

# 4.数据输出
print("========1.input rdd========")
print(input_rdd.take(2))
print(input_rdd.count())
print("========2.filter rdd========")
print(filter_rdd.take(2))
print(filter_rdd.count())
print("========3.result rdd1========")
print(result_rdd1)
print("========4.result rdd2========")
print(result_rdd2.max())  # 求最大
print(result_rdd2.min())  # 求最小
print(result_rdd2.mean())  # 求平均
print("========5.result rdd3========")
result_rdd3.foreach(lambda x: print(x))

# cache，不需要释放，用完就没了
# persist，它需要手动释放，blocking阻塞的意思是释放完了后，程序再往下进行
persist_rdd.unpersist(blocking=True)

# 5.关闭SparkContext
sc.stop()
# 程序结束时间：
endTime = time.time()
tatalTime = endTime - startTime
print(f'程序总耗时：{tatalTime}')


# spark容错--使用rdd血缘关系进行容错保障
# 每个操作都是返回一个rdd对象，当一个对象出现问题，可以再计算一次返回新的rdd

# 引出问题：
# 如果一个rdd被触发多次，第一个结果要用，第二个结果要用多次，多次创建rdd的问题出现---怎么解决？
# **问题：RDD依赖血缘机制保证数据安全，那每调用一次RDD都要重新构建一次，调用多次时性能就特别差，怎么办？**

# 解决重复利用rdd的冗余度--使用缓存机制
# **解决**：==**将RDD进行缓存**==，如果需要用到多次，将这个RDD存储起来，下次用到直接读取我们存储的RDD，不用再重新构建了

# 下面两种缓存都把依赖关系也给缓存进去：

# 1）cache
# - 功能：将RDD缓存在内存中
# - 语法：`rdd对象.cache()`
# - 本质：底层调用的还是persist（StorageLevel.MEMORY_ONLY），但是只缓存在内存，如果内存不够，缓存会失败
# - 场景：资源充足，需要将RDD仅缓存在内存中


# 2)persist
# - 功能：将**RDD**【包含这个RDD的依赖关系】进行缓存，可以**自己指定缓存的级别**【和cache区别】
# - 语法：`rdd对象.persist(StorageLevel)`
# - 级别：StorageLevel决定了缓存位置和缓存几份

# cache，不需要释放，用完就没了
# persist，它需要手动释放，blocking阻塞的意思是释放完了后，程序再往下进行


#**问题：为了避免重复构建RDD，可以将RDD进行persist缓存，但是如果缓存丢失，还是会重新构建RDD，
# 如果有1个RDD，会被使用多次，要经过非常复杂的转换过程才能构建，怎么解决？**

# 将rdd长期持久化存起来，因为有的rdd经过复杂的计算才得到的，所以要保持起来，且不保护rdd的依赖关系--解决方法：checkpoint机制

# 3）checkpoint机制
# 功能：将**RDD的数据**【不包含RDD依赖关系】存储在HDFS上


# 区别：
# 1.存储位置不同
# cache：存储在内存中
# Persist：利用Executor的内存和节点磁盘来存储
# Checkpoint：利用HDFS存储（不会丢失）

# 2.生命周期不同
# cache：随着任务的执行完成而消失
# Persist：如果程序结束或者遇到了unpersist，内存和磁盘中的缓存会被清理
# Checkpoint：只要不手动删除HDFS，就一直存在

# 3.存储内容不同
# cache：缓存整个RDD，保留RDD的依赖关系
# Persist：缓存整个RDD，保留RDD的依赖关系
# Checkpoint：存储是RDD的数据，不保留RDD的依赖关系


# **总结**：RDD的完整的容错机制，RDD的某个分区的数据如果丢失，则先去内存找回数据cache，如果内存没有，则去磁盘寻找persist，
# 如果磁盘没有则去HDFS找（checkpoint），如果HDFS也没有，则从头计算出这部分数据。




