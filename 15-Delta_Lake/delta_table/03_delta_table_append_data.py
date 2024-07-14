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

# 创建新的DataFrame，包含要插入的新数据
new_data = spark.createDataFrame([(1, "John"), (2, "Jane")], ["id", "name"])

# 将新数据追加到Delta表中
new_data.write.format("delta").mode("append").save("path_to_delta_table")


# 方法2：直写表名字
# 假设已经有一个名为delta_table的Delta表
# 创建一个新的DataFrame，包含要写入Delta表的数据
data = [("John", 25), ("Jane", 30)]
df = spark.createDataFrame(data, ["name", "age"])

# 使用saveAsTable方法将数据写入Delta表
df.write.format("delta").mode("append").saveAsTable("delta_table")

# 案例
from pyspark.sql import functions as F
df = spark.read.parquet("s3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/other_human_all_community_id_data_handled_Enhance_v1/")
print(f"数据源数据量：{df.count()}")
community_132_df = df.filter(F.col("community_id")==132)
normal_address_df = community_132_df.filter((F.col("address_1_2_like_count")==1)&(F.col("address_1_3_like_count")<=10)&(F.col("address_2_3_like_count")<=10)&(F.col("address_2_4_like_count")<=10)&(F.col("address_3_4_like_count")<=10)&(F.col("address_3_5_like_count")<=10))

list1 =["9aed82fa-b3b1-40b2-9426-5ed7d4c0bbad","0e6b397a-deac-47d2-91ca-ed487007d2dd","caa0840a-b17c-4947-9013-79b6a43a4719","65c891c2-21ec-43ac-8620-2dedd4c85c0c","9aed82fa-b3b1-40b2-9426-5ed7d4c0bbad"]

susp_address_df = spark.createDataFrame(((upmid,) for upmid in list1),["upmid"])
susp_address_df.display()


mid_df =normal_address_df.select("upmid").subtract(susp_address_df.select("upmid"))

new_df = normal_address_df.join(mid_df,normal_address_df.upmid==mid_df.upmid,"inner").drop(mid_df.upmid).select("upmid","address","community_id")
new_df.display()

# mid_df.display()

# 使用saveAsTable方法将数据写入Delta表
new_df.write.format("delta").mode("append").saveAsTable("airbot_prod.forter_whitelist")