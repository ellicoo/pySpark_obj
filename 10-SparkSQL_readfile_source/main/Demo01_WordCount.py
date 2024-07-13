from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：结构化流实现词频统计案例
   SourceFile  :	Demo01_WordCount
   Author      :	81196
   Date	       :	2023/9/19
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkSession
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("StructuredStreamingWordCount") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入(Source端)
# format("socket")：读取socket的数据
# host：socket的用户名
# port：socket的端口号
lines = spark.readStream.format("socket").option("host", "node1").option("port", 9999).load()

# 3.数据处理(Transformation)
# F.split(lines.value," ")，切割函数，把value列使用空格来切割
# F.explode(array)，爆炸函数，里面接受一个数组，把数组里的每一个单词单独返回（flatMap的效果）
wordCounts = lines.select(F.explode(F.split(lines.value, ",")).alias("word")).groupBy("word").count()

# 4.数据输出(Sink端)
# outputMode()：输出的数据模式，结构化流支持三种输出模式：（append，complete，update）
# format()：写出数据的位置，把数据写出到哪里去。console：标准输出，控制台。
query = wordCounts.writeStream.outputMode("complete").format("console")

# 5.启动流式任务
# start：启动流式任务
# awaitTermination：阻塞执行，等待任务的结果
query.start().awaitTermination()
