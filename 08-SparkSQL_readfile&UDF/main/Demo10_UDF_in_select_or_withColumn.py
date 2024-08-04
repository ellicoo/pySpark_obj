from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def merge_tags(new_df, old_df, fiveTagIDStr):
    # 1.如果new_df为空，返回old_df数据
    if new_df is None:
        return old_df
    # 2.如果old_df为空，返回new_df数据
    if old_df is None:
        return new_df

    # 3.如果两个都不为空，实现标签的合并
    # 3.1 new_df切割，得到一个列表，new_df_list
    new_df_list = str(new_df).split(",")
    # 3.2 old_df切割，得到一个列表，old_df_list
    old_df_list = str(old_df).split(",")

    # fiveTagIDStr字符串中，包含了所有5级标签的ID，使用(,)拼接，因此需要使用(,)切割

    # 因为可能以前userId打的是24这个标签--old_df的tag:24，现在更新了打25这个标签--new_df的tag:25，如果仅仅进行new_df的tag和old_df的tag进行合并去重就会出现互相矛盾的问题，
    # 比如访问周期，就会出现，即是0～7这个访问周期，又是7～14这个访问周期，而应该只保留最新的7～14这个tag周期。

    five_tag_id_list = fiveTagIDStr.split(",")
    # 通过当前这个4级标签种类(因为4级标签种类很多)下的所有5级标签，将原来的已经打的这个4级标签种类下的所有旧的5级标签全部剔除，old_df_list只保留其他4级标签种类的5级标签
    # 将以前的同类标签拔除，保留其他类别标签
    for tag in five_tag_id_list:
        if tag in old_df_list:
            old_df_list.remove(tag)

    # 3.3 把new_df_list和old_df_list进行合并，得到result_df_list
    result_df_list = new_df_list + old_df_list
    # 3.4 把最终的result_df_list以固定的符号拼接，返回
    return ",".join(set(result_df_list))


# UDF函数的使用：
# select与withColumn:
# (1)select:尽管select算子本身不是逐行操作，但它可以在内部调用逐行操作的UDF函数，因为Spark会将DataFrame的每一行数据传递给UDF进行处理,对已经有的列通过UDF产生新列通过alias来逐行命名新列名
# (2)withColumn: withColumn是一种逐行操作。可以用来对 DataFrame 的每一行应用一个函数，并创建一个新列或替换一个现有列

# 1，使用select来使用UDF
def merge_old_df_and_new_df(self, new_df, old_df, fiveTagIDStr):
    result_df = new_df.join(other=old_df, on=new_df['userId'] == old_df['userId'], how='left') \
        .select(new_df['userId'], merge_tags(new_df['tagsId'], old_df['tagsId'], fiveTagIDStr).alias("tagsId"))
    return result_df


# 2，使用withColumn来使用UDF
def merge_old_df_and_new_df(self, new_df, old_df, fiveTagIDStr):
    # 执行左连接
    joined_df = new_df.join(other=old_df, on=new_df['userId'] == old_df['userId'], how='left')

    # 使用 withColumn 来创建新列 tagsId，基于 merge_tags 函数
    result_df = joined_df.withColumn(
        'tagsId',
        merge_tags(F.col('new_df.tagsId'), F.col('old_df.tagsId'), fiveTagIDStr)
    ).select(
        F.col('new_df.userId'),  # 选择 userId 列
        F.col('tagsId')  # 选择新创建的 tagsId 列
    )

    return result_df


# 窗口函数
    window_spec = Window().partitionBy("LAUNCH_ID")

    # 此处只进行一次join操作，统计量的计算正确
    # 使用 F.expr 和 IF 进行窗口计算
    # df_with_counts = df.withColumn(
    #     "is_browse_cnt",
    #     F.count(F.expr("IF(is_browse = 1, user_id, NULL)")).over(window_spec)
    # )
    result_df = (
        joined_df.
            # 假如不存在第一层反欺诈，本统计的是所有参加本次活动的人数
            withColumn("participants", F.count("*").over(window_spec))
            # 假如不存在第一层反欺诈，本统计的是（参加本次活动且能抢到货的全体总人数）
            .withColumn("winner", F.count(F.when(F.col("ENTRY_STATUS") == "WINNER", 1)).over(window_spec))
            # 1.增加invalid
            # 假如存在第一层反欺诈，本统计的是：（本层认为是bot的总bot数）--关注投放“机器人数量”
            .withColumn("invalid", F.count(F.when(F.col("VALIDATION_RESULT") == "INVALID", 1)).over(window_spec))
            # 假如存在第一层反欺诈，本统计的是：(本层认为是human的总human数)-- 关注“真人数”(非bot)
            .withColumn("valid", F.count(F.when(F.col("VALIDATION_RESULT") == "VALID", 1)).over(window_spec))
            .withColumn("ratio_winner_to_valid", F.round(F.col("winner") / F.col("valid"), 3))

    )
    # 等价操作
    # result_df = joined_df.select(
    #     "*",
    #     F.count(F.expr("if(ENTRY_STATUS = 'WINNER', 1, NULL)")).over(window_spec).alias("winner"),
    #     F.count(F.expr("if(VALIDATION_RESULT = 'INVALID', 1, NULL)")).over(window_spec).alias("invalid"),
    #     F.count(F.expr("if(VALIDATION_RESULT = 'VALID', 1, NULL)")).over(window_spec).alias("valid"),
    #     F.round(F.count(F.expr("if(ENTRY_STATUS = 'WINNER', 1, NULL)")).over(window_spec),
    #     F.round(F.count(F.expr("if(ENTRY_STATUS = 'WINNER', 1, NULL)")).over(window_spec) /
    #                 F.count(F.expr("if(VALIDATION_RESULT = 'VALID', 1, NULL)")).over(window_spec), 3).alias("ratio_winner_to_valid")

    # 总结：
    # 使用 withColumn 和 when 函数确实在处理复杂的转换和中间步骤时更加灵活。它允许你一步步地添加列，并且更容易管理操作之间的依赖关系。
    # 虽然 select 和 F.expr 也很强大，但在处理表达式和操作顺序时可能需要更加小心
    # （1）withColumn 和 when：
    # 优点：逐步添加列，代码清晰，易于理解和维护。每个列的计算是独立的，并且你可以方便地调试每一步。
    # 缺点：多次调用 withColumn，每次都会生成一个新的 DataFrame 对象，可能会稍微影响性能。
    # （2）select 和 F.expr：
    # 优点：可以在一次 select 操作中完成所有的计算。可能在某些情况下，这种方式更高效，因为它避免了多次生成中间 DataFrame。
    # 缺点：较为复杂，不易于逐步调试和理解每个步骤的计算过程。表达式较长且不易于维护
