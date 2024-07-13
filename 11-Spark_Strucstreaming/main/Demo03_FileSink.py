from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：演示FliesSink的案例
   SourceFile  :	Demo03_FileSInk
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


# 4.数据输出
#format：FileSink（文件输出）需要指定输出不同的文件类型，比如csv、json、parquet、orc等
#path：指定文件的路径，这里要用路径！！！！
#checkpointLocation：指定流式任务的Checkpoint路径，路径会自动创建，不要提前创建好！！！！
query = input_df.writeStream\
    .outputMode("append")\
    .format("json")\
    .option("path","../data")\
    .option("checkpointLocation","../data/ckp")

# 5.启动流式任务
query.start().awaitTermination()
