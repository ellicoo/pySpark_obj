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

# 读取数据集并缓存
df = spark.read.csv("data.csv")
df.cache()  # 将数据集缓存到内存中

# 第一次使用缓存的 DataFrame
result1 = df.filter(df["column1"] > 100).groupBy("group_column").agg(F.avg("value_column"))
result1.show()

# 继续使用同一缓存的 DataFrame
result2 = df.filter(df["column2"] == "some_value").count()
print(result2)

# 可以进一步操作 df
result3 = df.groupBy("category").count()
result3.show()

# 最后释放缓存
df.unpersist()
