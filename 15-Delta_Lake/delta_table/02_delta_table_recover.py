# databricks上的spark环境没有，本脚本迁移需要补充spark环境
from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：演示RateSource速率数据源的使用
   SourceFile  :	Demo01_RateSource
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

from delta.tables import DeltaTable

# 定义Delta表的路径
table_path = 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'

# 获取Delta表的版本历史记录
delta_table = DeltaTable.forPath(spark, table_path)
version_history = delta_table.history()

# 打印版本历史记录
version_history.display()

# 恢复Delta表到指定的历史版本
# delta_table.restoreAsOf(version)


# 如果不想使用绝对地址
# sql最方便--直接查看delta表的历史版本
# describe history airbot_prod.forter_whitelist
# 回滚到指定版本
# RESTORE airbot_prod.forter_whitelist VERSION AS OF version_number
# restore airbot_prod.forter_whitelist version as of 版本号



# 查看指定版本的表

# 定义Delta表的路径
table_path = 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'

# 获取Delta表的特定版本
delta_table = DeltaTable.forPath(spark, table_path)
# df = delta_table.history().filter("version = version_number").select("version", "timestamp").as("delta_history")
df = delta_table.history().filter("version = version_number").select("version", "timestamp")
delta_table_as_of = delta_table.alias("delta_table").merge(df, "delta_table.version = delta_history.version").select("delta_table.*")

# 显示特定版本的数据
delta_table_as_of.show()