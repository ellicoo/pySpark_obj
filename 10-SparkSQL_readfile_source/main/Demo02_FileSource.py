from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：文件数据源
   SourceFile  :	Demo02_FileSource
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
    .appName("FileSourceLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入
input_df = spark.readStream \
    .format("csv") \
    .option("sep", ",") \
    .schema("id int,name string,age int,address string") \
    .load("../data/")

# 3.数据处理
# 略

# 4.数据输出
# append：可以用
# complete
query = input_df.writeStream.outputMode("append").format("console")

# 5.启动流式任务
query.start().awaitTermination()
