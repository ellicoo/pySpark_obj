"""
(1) 通过一个dataframe来创建一个delta表
"""
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

# %pip install delta-spark
from delta.tables import DeltaTable
df = spark.table("fraud_secure.forter_type1")
# 假设已经存在一个dataframe，先将这个dataframe存成delta格式的文件
df.write.format('delta').mode('overwrite').save('s3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path')

# 通过delta格式的文件来创建表
# 使用sql语句创建表

# %sql
# create table airbot_prod.forter_whitelist
# using delta
# location 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'


spark.sql("""
create table airbot_prod.forter_whitelist
using delta
location 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'

""")


# 使用delta表
# select * from airbot_prod.forter_whitelist




# 创建新的分区表
spark.sql("CREATE TABLE delta_partitioned (col1 STRING, col2 INT) USING DELTA PARTITIONED BY (partition_col STRING)")

# 从原始Delta表中读取数据，并按照分区列的值将数据写入新的分区表
spark.sql("INSERT INTO delta_partitioned PARTITION (partition_col) SELECT col1, col2, partition_col FROM original_delta_table")

# 验证新的分区表中的数据是否正确
spark.sql("SELECT * FROM delta_partitioned").show()
