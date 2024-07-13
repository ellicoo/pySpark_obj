from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
"""
-------------------------------------------------
   Description :	TODO：读取Kafka的数据
   SourceFile  :	Demo07_ReadFromKafka
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
#format('kafka')：读取Kafka中的数据
#kafka.bootstrap.servers：指定Kafka的broker地址
#subscribe：指定Kafka的topic，这个topic最好提前创建好
input_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","node1:9092,node2:9092,node3:9092")\
    .option("subscribe","test03")\
    .load()

# 3.数据处理
#select
# from table
# group by
#expr(sql)
#select(dsl)
#selectExpr(可以在dsl中写sql)
#result_df = input_df.selectExpr("cast(value as string)")
#withColunn：新增列或者重命名某个已存在的列
result_df = input_df.withColumn("value",F.expr("cast(value as string)"))


# 4.数据输出
query = result_df.writeStream.outputMode("append").format("console").option("truncate","False")

# 5.启动流式任务
query.start().awaitTermination()
