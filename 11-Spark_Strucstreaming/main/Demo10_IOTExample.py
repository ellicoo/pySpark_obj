from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
"""
-------------------------------------------------
   Description :	TODO：Spark读取Kafka的数据，统计信号强度大于30的设备信号
   SourceFile  :	Demo10_IOTExample
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
input_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers","node1:9092,node2:9092,node3:9092")\
    .option("subscribe","iot")\
    .load()

# 3.数据处理
print("=================1.DSL===================")
# #3.1 数据转换清洗
# filter_df = input_df.selectExpr("cast(value as string)")\
#     .select(F.json_tuple("value","deviceID","deviceType","deviceSignal","time")
#             .alias("deviceID","deviceType","deviceSignal","time"))
# #3.2使用DSL风格实现需求
# result_df = filter_df.where("deviceSignal > 30")\
#     .groupBy("deviceType")\
#     .agg(
#         F.count("deviceType").alias("type_cnt"),
#         F.avg("deviceSignal").alias("signal_avg")
#     )

print("=================2.SQL===================")
input_df.createOrReplaceTempView("t_source")
#json_tuple，它属于UDTF函数，类似于explode函数，所以需要使用lateral view的语法
result_df = spark.sql("""
    select
        deviceType,
        count(deviceType) as type_cnt,
        avg(deviceSignal) as signal_avg
    from t_source lateral view json_tuple(cast(value as string),"deviceID","deviceType","deviceSignal","time") as deviceID,deviceType,deviceSignal,`time`
    where deviceSignal > 30
    group by deviceType
""")
# 4.数据输出
query = result_df.writeStream.outputMode("complete").format("console").option("truncate","False")

# 5.启动流式任务
query.start().awaitTermination()
