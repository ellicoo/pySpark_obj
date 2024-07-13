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

# 定义列表
# my_list = ["value1", "value2", "value3"]
# result_address_list是一个list
# 创建DataFrame
result_address_list = ["value1", "value2", "value3"]
df = spark.createDataFrame([(value,) for value in result_address_list], ["upmid"])

# 打印DataFrame的内容
df.display()


# exmple_2
# 定义列表
my_list = [(1,"tea"),(2,"coffee"),(3,"milk")]

# 创建DataFrame
df = spark.createDataFrame(my_list, ["upmid","oder"])

# 打印DataFrame的内容
df.printSchema()
df.display()