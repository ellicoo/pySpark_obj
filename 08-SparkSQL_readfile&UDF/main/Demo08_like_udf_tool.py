from pyspark.sql import functions as F

"""
如果操作一个df的行时，需要将第一个df的行数据带入第二个df中进行计算，借助第二个df的计算返回一个值作为第一个df的新列值时候需要
"""
# from pyspark.sql import SparkSession

def get_source_community_address_like_count_df(source_df, specify_cpmmunity_df):
    # 获取第一个DataFrame，并构造header_2_str列
    source_df1 = source_df.withColumn(
        "header_2_str",
        F.concat(F.split(F.col("address"), " ")[0], F.lit(" "), F.split(F.col("address"), " ")[1])
    )

    df1 = source_df1.dropDuplicates(["community_id", "header_2_str"])

    # 获取第二个DataFrame，并重命名address列:filter(F.col("upmid")=="")
    df2 = specify_cpmmunity_df.filter(F.col("upmid").isNotNull()).withColumnRenamed("address", "df2_address")
    # df2 = specify_cpmmunity_df.filter(F.col("upmid")!="").withColumnRenamed("address", "df2_address")
    # 创建一个包含模糊匹配字符串的新列
    df1 = df1.withColumn("like_pattern", F.concat(F.lit("%"), F.col("header_2_str"), F.lit("%")))

    # 使用 join 进行连接
    joined_df = df1.join(df2, df1["community_id"] == df2["community_id"], how="left")

    # 过滤条件放在 join 后进行，并使用 SQL 表达式进行模糊匹配
    filtered_df = joined_df.filter(F.expr("df2_address LIKE like_pattern"))

    # 计算 address_like_count
    like_count_df = filtered_df.groupBy(
        df1["community_id"], df1["header_2_str"]
    ).agg(
        F.count("df2_address").alias("address_like_count")
    )

    # 将 like_count_df 与 df1 连接，得到最终结果
    result_df = source_df1.join(
        like_count_df,
        on=["community_id", "header_2_str"],
        how="left"
    ).dropDuplicates(["upmid", "community_id", "address"]).select(
        "upmid", "community_id", "address", "header_2_str", "address_like_count"
    )

    return result_df

# 示例调用
# 初始化 SparkSession
# spark = SparkSession.builder.appName("Example").getOrCreate()

# # 创建示例 DataFrame
# source_data = [(1, '123 Main St'), (2, '456 Maple Ave'), (3, '789 Broadway')]
# specify_data = [(1, '123 Main St Apt 1', 'upmid1'), (2, '456 Maple Ave Suite 2', 'upmid2'), (1, '123 Main St Apt 2', 'upmid3')]

# source_df = spark.createDataFrame(source_data, ["community_id", "address"])
# specify_cpmmunity_df = spark.createDataFrame(specify_data, ["community_id", "address", "upmid"])

# # 调用函数
# result_df = get_source_community_address_like_count_df(source_df, specify_cpmmunity_df)
# result_df.show()