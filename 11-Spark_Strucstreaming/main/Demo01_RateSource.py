from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：演示RateSource速率数据源的使用
   SourceFile  :	Demo01_RateSource
   Author      :	81196
   Date	       :	2023/9/21
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
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入
#rate：可以模拟数据源，源源不断地生产数据
#rowsPerSecond:每秒钟生存多少条数据，默认就是1条
#rampUpTime：经过多长时间后，达到生产速率，默认是0秒，这里设置为1秒钟
#numPartitions：分区数，默认是Spark的并行度的数量（分区数）
input_df = spark.readStream.format("rate")\
    .option("rowsPerSecond","1")\
    .option("rampUpTime","1")\
    .option("numPartitions","1")\
    .load()

# 3.数据处理


# 4.数据输出
#truncate:是否截断，默认为True，这里如果想看全部内容，要设置为False
result_df = input_df.writeStream.outputMode("append").format("console").option("truncate","False")

# 5.启动流式任务
result_df.start().awaitTermination()
