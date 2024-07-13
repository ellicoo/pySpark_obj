#需求：在PyCharm中使用standalone模式来运行wordcount词频统计案例。
from pyspark import SparkContext, SparkConf

import os
# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


#.1构建Spark环境
#setMaster：设置为独立模式：spark://node1:7077
conf = SparkConf().setAppName("WordCountStandalone").setMaster("spark://node1:7077")
sc = SparkContext(conf=conf)

#2.读取数据源
#textFile:读外部系统的文件
input_rdd = sc.textFile("hdfs://node1:8020/spark/wordcount/input/word_re.txt")

#3.数据处理
flat_map_rdd = input_rdd.flatMap(lambda line:line.split(" "))
map_rdd = flat_map_rdd.map(lambda word:(word,1))
result_rdd = map_rdd.reduceByKey(lambda x,y:x + y)

#4.数据输出
result_rdd.saveAsTextFile("hdfs://node1:8020/spark/wordcount/output1")

#5.停止Spark环境
sc.stop()

