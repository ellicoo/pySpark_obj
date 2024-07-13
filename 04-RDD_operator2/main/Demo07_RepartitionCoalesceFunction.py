from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示重分区算子
   SourceFile  :	Demo07_RepartitionCoalesceFunction
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
input_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# 3.数据处理
#3.1查看默认的分区数
print(f'默认的分区数为：{input_rdd.getNumPartitions()}')
print("========1.未调整之前的分区内元素=======")
print(input_rdd.glom().collect())

#3.2调大分区数
repartition_rdd = input_rdd.repartition(4)
print(f'调大后的分区数为：{repartition_rdd.getNumPartitions()}')
print("========2.调大分区后的分区内元素=======")
print(repartition_rdd.glom().collect())

#3.3调小分区数
coalesce_rdd = repartition_rdd.coalesce(1)
print(f'调小后的分区数为：{coalesce_rdd.getNumPartitions()}')
print("========3.调小分区后的分区内元素=======")
print(coalesce_rdd.glom().collect())

#3.4repartition 和 coalesce有何区别？
"""
    repartition:可以调大，也可以调小，一般用于调大分区。一定会经过Shuffle，底层走的是coalesce方法。
    coalesce：只能调小，不能调大。不走Shuffle。
"""

# 4.数据输出

# 5.关闭SparkContext
sc.stop()
