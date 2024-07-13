import re
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示广播变量综合案例
   SourceFile  :	Demo01_BroadCastExample
   Author      :	81196
   Date	       :	2023/9/11
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
#2.1 把特殊字符进行广播，在Driver端广播
broadcast_data = sc.broadcast([",", ".", "!", "#", "$", "%", ""])
#2.2 定义累加器
acc = sc.accumulator(0)
#2.3 读取数据
input_rdd = sc.textFile(get_absolute_path("../data/accumulator_broadcast_data.txt"))

# 3.数据处理
def map_line(word):
    #在Executor端获取特殊字符的数据
    lists = broadcast_data.value
    global acc
    if word in lists:
        #累加器+1
        acc.add(1)
        return False
    else:
        return True

# 处理rdd的函数直接调用
result_rdd = input_rdd.flatMap(lambda line:re.split("\s+",line))\
    .filter(lambda word:map_line(word))\
    .map(lambda word:(word,1))\
    .reduceByKey(lambda x,y: x + y)

# 4.数据输出
print(f"词频统计结果为：{result_rdd.collect()}")
print(result_rdd.take(3))
print(f"特殊字符的次数为：{acc.value}")

# 算子：
# 1）转换类算子--返回的结果仍是rddd对象的算子--从一个rdd变成另外一个rdd
#               如果没有action算子，那转换类算子是不工作的--懒加载
# 2）action算子--返回值不是rdd对象的算子，如collect等

#  转换类算子相当于构建执行计划 而action则是执行计划的开关


# 5.关闭SparkContext
sc.stop()
