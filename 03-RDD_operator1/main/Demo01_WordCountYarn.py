import re

from pyspark import SparkContext,SparkConf
import os
# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

#1.构建Spark环境
conf = SparkConf().setAppName("WordCountYarn").setMaster("yarn")
sc = SparkContext(conf=conf)

#2.读取数据
input_rdd = sc.textFile("hdfs://node1:8020/spark/wordcount/input/word_re.txt")

#3.数据处理（结果）
# lambda x,y:x + y -- 定义一个双参数的匿名函数，接收rdd的两个元素存在x和y中
result_rdd = input_rdd\
    .flatMap(lambda line:re.split('\s+',line)) \
.map(lambda line:(line,1)).reduceByKey(lambda x,y:x + y)


#4.数据写出（输出）
#result_rdd.foreach(lambda x:print(x))
result_rdd.saveAsTextFile("hdfs://node1:8020/spark/wordcount/output2")

#5.停止环境
sc.stop()
