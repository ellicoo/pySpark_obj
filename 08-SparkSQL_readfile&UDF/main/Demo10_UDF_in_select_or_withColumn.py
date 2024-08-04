from pyspark.sql.functions import F


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
