from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：使用jdbc的方式读写MySQL
   SourceFile  :	Demo03_ReadWriteJDBC
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
#读MySQL，方式一，spark.read.jdbc
#数据库的url路径
url = "jdbc:mysql://node1:3306"
#表名称，写法是：数据库名.表名
tbl = "db_company.emp"
#连接数据库的用户名和密码，是一个dict类型
prop = {"user":"root","password":"123456"}
input_df = spark.read.jdbc(url=url,table=tbl, properties=prop)
#读mysql，方式二，spark.read.format("jdbc")
#dbtable：数据库名.表名
#user：用户名
#password：密码
input_df2 = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",tbl)\
    .option("user","root")\
    .option("password","123456")\
    .load()

# 3.数据处理
#数据写入df.write.jdbc，方式一
url2 = "jdbc:mysql://node1:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true"
input_df.write.jdbc(url=url2,table="db_company.emp_v2",mode='overwrite',properties=prop)
#数据写入df.write.format("jdbc")，方式二
# input_df2.write.format("jdbc")\
#     .option("url",url2)\
#     .option("dbtable","db_company.emp_v2")\
#     .option("user","root")\
#     .option("password","123456")\
#     .mode("overwrite")\
#     .save()

# 4.数据输出
input_df.printSchema()
input_df.show()
# input_df2.printSchema()
# input_df2.show()

print("==写入MySQL==")

# 5.关闭SparkContext
spark.stop()


