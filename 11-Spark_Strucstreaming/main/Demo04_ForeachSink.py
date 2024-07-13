from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：演示foreachSink案例
   SourceFile  :	Demo04_ForeachSink
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
#4.1普通函数输出
#query = input_df.writeStream.outputMode("append").foreach(lambda x:print(x))
#4.2使用对象输出
class RowPrinter:
    #open方法，一般用于开辟连接操作，仅执行一次，可选的方式
    def open(self, partition_id, epoch_id):
        print("打开jdbc连接......")
        return True
    #核心方法，必须有，用于数据处理
    def process(self, row):
        print("数据处理方法......")
        print(row)
    #close方法，一般用于关闭连接操作，仅执行一次，可选的方法
    def close(self, error):
        print("关闭jdbc连接......")
query = input_df.writeStream.outputMode("append").foreach(RowPrinter())

# 5.启动流式任务
query.start().awaitTermination()
