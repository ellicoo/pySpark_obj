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