from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示聚合函数
   SourceFile  :	Demo02_AggregateFunctions
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
input_rdd = sc.parallelize([('a',1),('b',1),('a',1)])


# aggregateByKey(zeroValue)(seqOp,combOp)
# 作用： 根据key对value做聚合操作。
# 先在分区内聚合，再根据分区内聚合结果在分区间聚合。
# 第一个参数zeroValue表示在分区内计算时的初始值（零值）；
# 第二个参数的seqOp表示分区内计算规则；
# 第二个参数的combOp表示分区间计算规则。


# 3.数据处理
#3.1 分组，没有聚合
result_rdd1 = input_rdd.groupByKey()
from operator import add

# （1）aggregateByKey：

# 适合场景：求各个分区的最大值（分区内自定义一个求最值的计算逻辑），
#         并把各个分区的最大值进行相加（分区间可以自定义求和的计算逻辑）。

# 初始值为0的情况，是正常的情况：
# 第一个参数：初始值
# 第二个参数：分区内操作逻辑，第一个参数的初始值参加计算
# 第三个参数：分区间操作逻辑，且第一个参数的初始值不参加计算
result_rdd2 = input_rdd.aggregateByKey(0,add,add)
#初始值为1的情况，分区内会进行累加
result_rdd3 = input_rdd.aggregateByKey(1,add,add)

# （2）foldByKey  ：
#  def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
# 第一个参数：初始值
# 第二个参数：计算函数
#初始值为0的情况,是正常的情况

result_rdd4 = input_rdd.foldByKey(0,add)

#初始值为1的情况,分区内会进行累加

result_rdd5 = input_rdd.foldByKey(1,add)

# （3）reduceByKey：这是上面两种写法的最好方案，当然灵活性变差。大多数情况够用
#reduceByKey，简化写法，掌握这种写法
result_rdd6 = input_rdd.reduceByKey(add)
# 4.数据输出--def mapValues(self, f): 其中f为值的处理函数，list函数就是将值以列表的形式追加而非进行聚合运算

print(f"groupByKey的结果为：{result_rdd1.mapValues(list).collect()}")

print(f"aggregateByKey算子，初始值为0的结果为：{result_rdd2.collect()}")
print(f"aggregateByKey算子，初始值为1的结果为：{result_rdd3.collect()}")
print(f"foldByKey算子，初始值为0的结果为：{result_rdd4.collect()}")
print(f"foldByKey算子，初始值为1的结果为：{result_rdd5.collect()}")
print(f"reduceByKey算子，没有初始值的结果为：{result_rdd6.collect()}")


# 5.关闭SparkContext
sc.stop()
