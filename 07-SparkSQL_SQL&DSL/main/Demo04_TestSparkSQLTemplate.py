from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo04_TestSparkSQLTemplate
   Author      :	81196
   Date	       :	2023/9/12
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
    .getOrCreate()

# 2.数据输入
# 这个spark.table操作就是会在executor的JVM虚拟机的堆中创建一个初始的RDD类对象来存储"bot_master_all_risk_level_score"表的数据
risk_df = spark.table("airbot_prod.bot_master_all_risk_level_score").filter(
    (F.col("score") >= 35) & (F.col("human") != 1)).dropDuplicates(["identity_upm"])

# 3.数据处理
# 不包含逻辑
# 每次进行转换类算子的计算--如filter等，都是通过对旧的RDD类对象进行计算操作，然后在JVM中又创建一个新的RDD类的子类对象，然后用这个子类对象储存旧RDD类对象的计算结果
df = risk_df.filter(~F.array_contains("reason", "session_flag"))
# 包含逻辑
# df = risk_df.filter(~F.array_contains("reason", "session_flag"))
df = df.select(
    "identity_upm",
    "reason"
)

# 4.数据输出


# 5.关闭SparkContext
spark.stop()
