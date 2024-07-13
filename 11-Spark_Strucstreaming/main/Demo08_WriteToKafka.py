from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：写数据到Kafka中
   SourceFile  :	Demo08_WriteToKafka
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
input_df = spark.readStream.format("socket").option("host","node1").option("port","9999").load()

# 3.数据处理
#input_df：value


# 4.数据输出
#format('kafka')：把数据写出到Kafka中
#kafka.bootstrap.servers：Kafka的broker地址
#topic：Kafka的topic
#checkpointLocation：往外部介质中输出数据，需要Checkpoint
query = input_df.writeStream\
    .outputMode("append")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","node1:9092,node2:9092,node3:9092")\
    .option("topic","test04")\
    .option("checkpointLocation","../data/ckp3")

# 5.启动流式任务
query.start().awaitTermination()
