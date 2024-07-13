"""
(1) 通过一个dataframe来创建一个delta表
"""

%pip install delta-spark
from delta.tables import DeltaTable

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
select * from airbot_prod.forter_whitelist




# 创建新的分区表
spark.sql("CREATE TABLE delta_partitioned (col1 STRING, col2 INT) USING DELTA PARTITIONED BY (partition_col STRING)")

# 从原始Delta表中读取数据，并按照分区列的值将数据写入新的分区表
spark.sql("INSERT INTO delta_partitioned PARTITION (partition_col) SELECT col1, col2, partition_col FROM original_delta_table")

# 验证新的分区表中的数据是否正确
spark.sql("SELECT * FROM delta_partitioned").show()
