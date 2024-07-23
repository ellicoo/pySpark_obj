from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

"""
-------------------------------------------------
   Description :	TODO：读写CSV文件案例
   SourceFile  :	Demo01_ReadCsv
   Author      :	81196
   Date	       :	2023/9/14
-------------------------------------------------
checkpoint 适合在长时间运行的作业中使用，它会将数据写入到分布式文件系统中，
例如 HDFS 或 S3 等。这样做可以防止作业失败时的数据丢失，并且在作业重启时
能够从 checkpoint 处继续计算，而不是从头开始。

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

# 设置 checkpoint 的存储路径
spark.sparkContext.setCheckpointDir("hdfs://path/to/checkpoint/dir")

# 创建一个DataFrame
df = spark.read.csv("file.csv", header=True)

# 对DataFrame进行一系列复杂计算，并进行 checkpoint
df_filtered = df.filter(df["column"] > 100)
df_filtered_checkpointed = df_filtered.checkpoint()

# 后续可以直接使用 checkpoint 后的 DataFrame 进行操作
df_grouped = df_filtered_checkpointed.groupBy("group_column").agg(F.avg("value_column"))

# 注意：checkpoint 后的 DataFrame 是一个新的 DataFrame 对象
# 如果不再需要 checkpoint 后的 DataFrame，可以选择手动删除 checkpoint
df_filtered_checkpointed.unpersist()
