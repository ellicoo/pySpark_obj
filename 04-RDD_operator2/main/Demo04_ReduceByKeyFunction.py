from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：演示reduceByKey算子
   SourceFile  :	Demo03_GroupByKeyFunction
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
input_rdd = sc.textFile(get_absolute_path("../data/word.txt"))

# 3.数据处理
#groupByKey：先ByKey，再group，没有聚合的功能
#reduceByKey：先ByKey，再reduce，有聚合的功能
flat_map_rdd = input_rdd.flatMap(lambda line:line.split("|"))
map_rdd = flat_map_rdd.map(lambda word:(word,1))
result_rdd = map_rdd.reduceByKey(lambda item,tmp:item + tmp)

# 4.数据输出
result_rdd.foreach(lambda x:print(x))

# 5.关闭SparkContext
sc.stop()
