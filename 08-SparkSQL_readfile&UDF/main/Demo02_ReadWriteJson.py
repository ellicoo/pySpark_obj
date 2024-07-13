from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：读写Json文件
   SourceFile  :	Demo02_ReadWriteJson
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
#读json文件,方式一：spark.read.json
input_df = spark.read.json(path="../data/people.json")
#读json文件，方式二：spark.read.format(“json”)
input_df2 = spark.read.format("json").load("../data/people.json")

# 3.数据处理
#写json文件,方式一：df.write.json
input_df.write.json(path='../data/output/json01',mode="overwrite")
#写json文件，方式二：df.write.format("json")
input_df2.write.format("json").mode("overwrite").save("../data/output/json02")

# 4.数据输出
input_df.printSchema()
input_df.show()
input_df2.printSchema()
input_df2.show()

print("==写出json文件==")



# 5.关闭SparkContext
spark.stop()
