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

# 加载Delta表为DataFrame
delta_table = DeltaTable.forPath(spark, table_path)
df = delta_table.toDF()

# 创建一个新的DataFrame，包含要更新的数据
update_data = spark.createDataFrame([(1, "John"), (2, "Jane")], ["id", "name"])

# 使用merge操作将更新的数据合并到Delta表中
delta_table.alias("target").merge(
    update_data.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate().updateAll().execute()

# 查看更新后的Delta表数据
df_after_update = delta_table.toDF()
df_after_update.show()