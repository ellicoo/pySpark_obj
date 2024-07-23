from pyspark import StorageLevel
from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：读写CSV文件案例
   SourceFile  :	Demo01_ReadCsv
   Author      :	81196
   Date	       :	2023/9/14
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

# 读取数据集并持久化到内存和磁盘中，使用序列化存储
df = spark.read.csv("data.csv")
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# 第一次使用 df，Spark 会计算并将结果缓存
result1 = df.filter(df["column1"] > 100).groupBy("group_column").agg(avg("value_column"))
result1.show()

# 第二次使用相同的 df，Spark 将直接从缓存中获取数据，而不会重新计算
result2 = df.filter(df["column2"] == "some_value").count()
print(result2)

# 可以继续对 df 进行操作，Spark 会复用已经持久化的数据
result3 = df.groupBy("category").count()
result3.show()

# 最后释放持久化的资源
df.unpersist()
