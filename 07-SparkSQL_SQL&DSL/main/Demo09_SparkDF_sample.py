from pyspark.sql import functions as F
# 导入相关模块
from pyspark.sql import SparkSession
"""
有时候需要对数据进行抽样
"""
# 创建SparkSession
spark = SparkSession.builder.getOrCreate()

# 读取数据到DataFrame
dfjoined = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header=True, inferSchema=True)
# 对DataFrame进行随机抽样
# 参数解释：
# - fraction: 抽样比例，比如0.2表示抽取20%的数据
# - withReplacement: 是否放回抽样，True表示有放回，False表示无放回
# - seed: 随机种子，用于保证结果的可重复性
less_25_df = dfjoined.filter(F.col("address_2_3_like_count")<=25)
df = dfjoined.filter(F.col("address_2_3_like_count")==25)
print(f"address_2_3_like_count <= 25的总数据量为：{less_25_df.count()}")
print(f"address_2_3_like_count = 25的总数据量为：{df.count()}")

sampled_df = df.sample(withReplacement=False, fraction=0.2, seed=1234)

# 显示抽样结果
sampled_df.display()