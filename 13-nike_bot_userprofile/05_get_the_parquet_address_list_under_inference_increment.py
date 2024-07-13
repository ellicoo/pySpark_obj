import boto3

def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    生成 S3 存储桶中与指定前缀和后缀匹配的对象。

    :param bucket: S3 存储桶的名称。
    :param prefix: 只获取以此前缀开头的对象的前缀（可选）。
    :param suffix: 只获取以此后缀结尾的对象的后缀（可选）。
    """
    # 创建一个新的 AWS session
    session = boto3.Session()
    # 创建 S3 客户端

    s3 = session.client('s3')
    # 创建 S3 分页器
    paginator = s3.get_paginator('list_objects_v2')

    kwargs = {'Bucket': bucket, 'Prefix': prefix}

    # 遍历所有页面
    for page in paginator.paginate(**kwargs):
        try:
            contents = page['Contents']
        except KeyError:
            # 如果页面没有内容，则返回
            return
        # 遍历页面中的每个对象
        for obj in contents:
            key = obj['Key']
            # 如果对象的键以指定的前缀开头并且以指定的后缀结尾，则生成该对象
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

def get_inference_paths(bucket, prefix, filename):
    """
    获取包含指定文件名的推断文件夹中的对象路径列表。

    :param bucket: S3 存储桶的名称。
    :param prefix: S3 对象的前缀。
    :param filename: 要匹配的文件名。
    """
    paths = []
    # 遍历匹配指定前缀的 S3 对象
    for obj in get_matching_s3_objects(bucket, prefix=prefix):
        key = obj['Key']
        # 如果对象的键以指定的前缀开头并且包含指定文件名，则生成该对象路径
        if key.startswith(prefix) and filename in key:
            # 从对象键中提取推断文件夹的路径
            inference_folder_path = '/'.join(key.split('/')[:-2])
            # 如果推断文件夹路径中包含 "inference_datetime_"，则添加到路径列表中
            if 'inference_datetime_' in inference_folder_path:
                # 构建对象路径并添加到路径列表中
                s3_path = f's3://{bucket}/{inference_folder_path}/{filename}'
                paths.append(s3_path)
    return paths

# # 指定 S3 存储桶和前缀
# bucket = 'ngap--customer-data-science--prod--us-east-1'
# prefix = 'airbot/uat/bot_detection_nlp/data/address_analysis_datetime_20240202_143955/'
# filename = 'address_analysis_data_address_processed.parquet'

# # 获取指定前缀下的路径列表
# s3_paths = get_inference_paths(bucket, prefix, filename)

# # 去除重复项
# s3_paths = list(set(s3_paths))

# # 升序排序
# s3_paths.sort()

# # 打印 S3 path列表数量
# print(len(s3_paths))
# print(s3_paths)


def get_mid_processed_df():
    # 第一步：获取inference的所有parquet
    # 指定 S3 存储桶和前缀
    bucket = 'ngap--customer-data-science--prod--us-east-1'
    prefix = 'airbot/uat/bot_detection_nlp/data/address_analysis_datetime_20240202_143955/'
    filename = 'address_analysis_data_address_processed.parquet'

    # 获取指定前缀下的路径列表
    s3_paths = get_inference_paths(bucket, prefix, filename)

    # 去除重复项
    s3_paths = list(set(s3_paths))

    # 升序排序
    s3_paths.sort()

    # 第二步：获取zicode问题的parquet
    zipcode_suspicous_df = spark.read.parquet(
        "s3://ngap--customer-data-science--prod--us-east-1/airbot/uat/bot_detection_nlp/data/address_analysis_datetime_20240202_143955/base/address_analysis_data_address_processed.parquet")

    # 第三步：union所有parquet结果
    # 创建一个空的 DataFrame 用于存储所有读取结果
    all_data_df = None

    # 先union所有inference的
    # 遍历列表中的每个 S3 路径
    for s3_path in s3_paths:
        # 使用 Spark 读取 Parquet 文件
        data_df = spark.read.parquet(s3_path)

        # 如果 all_data_df 为空，则将 data_df 赋值给 all_data_df
        if all_data_df is None:
            all_data_df = data_df
        # 否则，将 data_df 与 all_data_df 进行 union
        else:
            all_data_df = all_data_df.union(data_df)

    # 再union zipcode出现问题的parquet
    all_data_df = all_data_df.union(zipcode_suspicous_df)

    # 去重
    all_data_df = all_data_df.dropDuplicates(["forter_address__shipping_address", "forter_address__account_id"])

    # print(all_data_df.count())
    # all_data_df.display()

    return all_data_df

# get_mid_processed_df().display()
# 暂时不用
def get_new_df(new_account_id_df):
    # 增量设计
    # 先找出新数据
    # 处理后的全量数据_包含新数据
    # query_str="""
    #       select forter_address__account_id_list_for_the_address as account_id,address from airbot_prod.jp_community
    #       """
    # all_df= spark.sql(query_str)

    # 历史数据旧数据
    # history_df = spark.table("airbot_prod.jp_community_tag_dev")


    # 找出all_df中存在的account_id，但在history_df中不存在的account_id
    # new_account_id_df = all_df.select("account_id").subtract(history_df.select("account_id"))

    # 将new_account_id_df与dfA进行连接，筛选出id在diffIds中的记录
    # new_df = all_df.join(new_account_id_df,
    #                         all_df.account_id == new_account_id_df.account_id,
    #                     "inner").drop(new_account_id_df.account_id)

    # 将找到的新数据去源头筛选关联数据,从mid_df中筛选新的数据
    # 会得到稍微多的数据
    mid_df = get_mid_processed_df()
    # 只取增量数据
    new_df = mid_df.join(new_account_id_df,
                            mid_df.forter_address__account_id == new_account_id_df.account_id,
                        "inner").drop(new_account_id_df.account_id)
    new_df=new_df.dropDuplicates(["forter_address__shipping_address","forter_address__account_id"])
    return new_df

# get_new_df().display()

# z增量数据紧紧这个位置有区别
def get_type1_zipcode_shipping_address_processed_address_df(new_account_id_df):
    from pyspark.sql import functions as F
    import re

    # 全量数据
    # mid_processed_df=get_mid_processed_df()

    # 增量数据 打标签
    mid_processed_df = get_new_df(new_account_id_df)

    print(f"中间表数量：{mid_processed_df.count()}")

    # 获取zipcode的table
    type1_df = spark.table("fraud_secure.forter_type1").filter(
        (F.col("ship_country") == "JP") & (F.col("account_id").isNotNull())
    ).select(
        "account_id",
        "ship_zip",
        "shipping_address"
    )

    joined_df = mid_processed_df.join(
        type1_df,
        ((F.col("forter_address__account_id") == F.col("account_id")) & (
                    F.col("forter_address__shipping_address") == F.col("shipping_address"))),
        how="inner"
    ).dropDuplicates(["account_id", "shipping_address", "address"]) \
        .select(
        "account_id",
        "shipping_address",
        # "forter_address__shipping_address",
        # "processed_address",
        "address",
        "ship_zip"
        # "community_id"
    ).withColumnRenamed("address", "processed_address")

    #
    def get_zipcode_substring(zipcode):
        if zipcode:
            # 使用正则表达式提取数字
            numbers = re.findall(r'\d+', zipcode)
            # 将提取到的数字列表转换为字符串
            result = ''.join(numbers)
            return result
        return None

    # 注册函数1:1
    get_zipcode_udf = F.udf(get_zipcode_substring)

    postal_code_df = joined_df.withColumn("jp_zipcode", get_zipcode_udf(F.col("ship_zip")))

    # 获取字符串有几个数字
    def get_len_postal_code(postal_code):
        if postal_code:
            # 使用len函数获取数字的个数
            return len(postal_code)
        return None

    get_len_postal_code_udf = F.udf(get_len_postal_code)

    final_df = postal_code_df.withColumn("zipcode_digits", get_len_postal_code_udf(F.col("jp_zipcode")))

    final_df = final_df.select(
        "account_id",
        "shipping_address",
        # "forter_address__shipping_address",
        "processed_address",
        # "address",
        "ship_zip",
        "jp_zipcode",
        "zipcode_digits"
        # "community_id"
    )
    print(f"joined type1 后的数量：{final_df.count()}")
    # display()
    return final_df
