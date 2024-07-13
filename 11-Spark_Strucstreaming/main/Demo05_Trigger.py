from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
"""
-------------------------------------------------
   Description :	TODO：演示trigger（触发器）的案例
   SourceFile  :	Demo05_Trigger
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
result_df = input_df.select(F.explode(F.split("value"," ")).alias("word")).groupBy("word").count()

# 4.数据输出
#processingTime：以固定的时间间隔运行
#query = result_df.writeStream.outputMode("complete").format("console").trigger(processingTime='5 seconds')
#once：只执行一次，就是批处理，不用
query = result_df.writeStream.outputMode("complete").format("console").trigger(once=True)
#continuous：以固定的时间周期性运行（持续），但是尚不成熟，不用
#query = input_df.writeStream.outputMode("append").format("console").trigger(continuous='5 seconds')

# 5.启动流式任务
query.start().awaitTermination()
