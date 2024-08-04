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

# 2.数据输入（RDD同理）--人话：
# 这个spark.table操作就是会在driver的JVM虚拟机的堆中创建一个全局的DataFrame类对象，来假装存储"bot_master_all_risk_level_score"表的数据
# 然后driver进程会对这个全局的DataFrame类对象进程分区操作，即生成一个对象数组，数组的每个元素就是这个全局DataFrame类对象的一部分，即数组的每个元素都是DataFrame分区对象。
# driver会进行DAG逻辑划分，将"对每个对象数组元素--分区对象进行的操作打包成一个task对象，如果在 DataFrame 上连续应用 map 和 flatMap 操作，
# Spark 会将这些操作合并到一个 Task 中执行。这意味着，在处理单个 DataFrame 分区时，这些操作是串行的，不会被分开为多个 Task
# 然后driver进程会将这个task对象，和，对应的driver JVM中的对象数组中的某个分区 DataFrame对象发送给各executor进程，executor进程会在JVM中初始化这个RDD分区对象并对这个RDD分区进行task的操作

# 2. 行话：
# `spark.table` 操作会在 Driver 的 JVM 中创建一个全局的 `DataFrame` 对象，表示 "bot_master_all_risk_level_score" 表的数据
# Driver 对这个 `DataFrame` 进行逻辑分区，生成多个分区对象。实际的数据加载和处理是在执行阶段进行的。
# Driver 会生成 DAG 执行计划，将对每个分区的操作打包成 Task 对象。如果对 `DataFrame` 进行连续的转换操作，Spark 会将这些操作合并到一个 Task 中执行。
# Driver 将 Task 和分区信息发送给 Executors，Executors 在其 JVM 中初始化分区数据并执行任务操作。

# 总结：
# Task 是全局 DataFrame 操作的声明（操作指令），定义了如何处理数据。
# 这些 Task 会被应用到 DataFrame 的每个分区上，每个 Executor 在其负责的分区上执行 Task，从而完成对整个 DataFrame 的操作

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
