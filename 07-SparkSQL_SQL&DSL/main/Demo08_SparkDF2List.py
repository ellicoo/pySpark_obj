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

# 创建DataFrame
# 将spark的一列数据变成列表
data = [("John", 25), ("Doe", 30), ("Alice", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# 选择需要收集的列并调用collect()方法
# name_list = df.collect() # 取出的列表元素是个行对象:[Row(name='John', age=25), Row(name='Doe', age=30), Row(name='Alice', age=28)]
# name_list = df.rdd.flatMap(lambda x: x).collect() # 会将行的所有值展平成一个列表,只取值，不取Row行对象: ['John', 25, 'Doe', 30, 'Alice', 28]
name_list = df.select(df.name).rdd.flatMap(lambda x: x).collect() # 会将行的所有值展平成一个列表,这里只有一列数据，只取值，不取行对象:['John', 'Doe', 'Alice']
# for row in name_list:
#   print(row)
print(name_list)


