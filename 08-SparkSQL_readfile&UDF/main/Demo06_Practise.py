from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo06_Practise
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

# 2.数据输入
input_df = spark.read.csv(path='../data/data.txt',sep='|',schema='id int,name string,sex string,address string')

# 3.数据处理
#result_df = input_df.groupBy('address').agg(F.count('address').alias("cnt"))
result_df = input_df.groupBy('address').count()

# 4.数据输出
input_df.printSchema()
input_df.show()
result_df.show()
# 5.关闭SparkContext
spark.stop()
