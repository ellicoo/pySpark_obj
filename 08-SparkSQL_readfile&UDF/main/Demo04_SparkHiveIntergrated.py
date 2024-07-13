from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo04_SparkHiveIntergrated
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
#hive.metastore.warehouse.dir:HDFS数据的存储地址
#hive.metastore.uris：Hive metastore的地址
#enableHiveSupport()：开启Hive支持
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config('hive.metastore.warehouse.dir','hdfs:///user/hive/warehouse/') \
    .config('hive.metastore.uris','thrift://node1:9083')\
    .enableHiveSupport()\
    .getOrCreate()

# 2.数据输入
spark.sql("show databases").show()
spark.sql("use insurance_dw").show()
spark.sql("show tables").show()
# spark.sql("select * from test_table").show()

# 3.数据处理


# 4.数据输出

# 5.关闭SparkContext
spark.stop()
