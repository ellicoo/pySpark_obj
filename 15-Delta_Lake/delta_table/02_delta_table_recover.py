
from delta.tables import DeltaTable

# 定义Delta表的路径
table_path = 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'

# 获取Delta表的版本历史记录
delta_table = DeltaTable.forPath(spark, table_path)
version_history = delta_table.history()

# 打印版本历史记录
version_history.display()

# 恢复Delta表到指定的历史版本
delta_table.restoreAsOf(version)


# 如果不想使用绝对地址
# sql最方便--直接查看delta表的历史版本
describe history airbot_prod.forter_whitelist
# 回滚到指定版本
# RESTORE airbot_prod.forter_whitelist VERSION AS OF version_number
restore airbot_prod.forter_whitelist version as of 版本号



# 查看指定版本的表

# 定义Delta表的路径
table_path = 's3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/forter_whitelist_delta_table_path'


# 获取Delta表的特定版本
delta_table = DeltaTable.forPath(spark, table_path)
df = delta_table.history().filter("version = version_number").select("version", "timestamp").as("delta_history")
delta_table_as_of = delta_table.alias("delta_table").merge(df, "delta_table.version = delta_history.version").select("delta_table.*")

# 显示特定版本的数据
delta_table_as_of.show()