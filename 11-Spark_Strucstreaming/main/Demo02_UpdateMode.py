from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：演示Update模式
   SourceFile  :	Demo02_UpdateMode
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
#input_df = input_df.where("value > 20")
input_df = input_df.groupBy("value").count()

# 4.数据输出
#append：不支持聚合操作，会显示全量数据
#complete：仅支持聚合操作，它会显示全部的聚合结果
#update：支持聚合和非聚合操作，仅显示新增的数据（第一次插入和对历史数据更新）
#工作中怎么选择？
#如果结果需要聚合，则可以选择complete和update模式，根据是否需要全部显示结果来决定最终的模式
#如果结果不需要聚合，则可以选择append或者Update模式，根据是否需要全部显示结果来决定最终的模式
query = input_df.writeStream.outputMode("update").format("console")

# 5.启动流式任务
query.start().awaitTermination()
