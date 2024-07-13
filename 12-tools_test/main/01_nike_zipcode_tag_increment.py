# databricks 上执行：
# %pip install -U spacy==3.6.1 ginza ja_ginza
# %pip install --upgrade scipy networkx
# %pip install pydantic
# %pip install urllib3
# # 处理出出现：ModuleNotFoundError: No module named 'click.exceptions'
# # 在运行过程中试图使用了 spacy 库，而这个库依赖于 click 库。然而，在你的环境中找不到 click 库
# # 有时候可以执行成功，这取决于我的作业执行的环境和资源分配情况。有时候，作业可能在主节点上运行，而主节点上已经安装了所需的库，因此不会遇到问题，暂时就没有爆相关的错误。
# %pip install click

import numpy as np
import pandas as pd
from collections import Counter
import itertools
import seaborn as sns
import networkx as nx
import matplotlib as plt
import collections
# UDF需要
import urllib3
# 用了 spacy 库，而这个库依赖于 click 库
import click

import spacy
nlp = spacy.load("ja_ginza")


import re

def is_digit_or_alpha_char(char):
    # 定义用于检查字符串的正则表达式
    pattern = r"^[a-zA-Z0-9]+$"

    # 检查字符串是否只包含数字和字母
    return re.match(pattern, char)

def is_japanese_char(char):
    # 定义日语字符的 Unicode 范围
    japanese_char_pattern = r'[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9faf]'
    # 检查字符是否匹配
    return re.match(japanese_char_pattern, char) is not None

def join_strings(str_list):
    # 结果列表
    result = []
    # 临时字符串，用于拼接
    temp = str_list[0]

    # 遍历列表中的字符串（从第二个开始）
    for s in str_list[1:]:
        # 检查当前temp的最后一个字符和s的第一个字符
        if (temp[-1] != '市' and is_japanese_char(temp[-1]) and is_japanese_char(s[0])) \
            or (is_digit_or_alpha_char(temp) and is_digit_or_alpha_char(s)):
            # 如果是日语字符或者数字/字母，则拼接
            temp += s
        else:
            # 否则，将temp添加到结果列表，并重新开始新的拼接
            result.append(temp)
            temp = s

    # 不要忘记将最后一个temp添加到结果列表
    result.append(temp)

    return result

def split_on_japan(input_str):
    # Regular expression pattern: (Japan)
    # The parentheses around 'Japan' ensure that the delimiter is captured and included in the result.
    pattern = r'(Japan)'

    # re.split splits the string at 'Japan' and includes 'Japan' in the result
    ret = re.split(pattern, input_str)
    return [each for each in ret if each]


def split_at_last_dash(s):
    # 检查字符串是否只包含数字和'-', 且恰好包含3个'-'
    if re.match(r'^[0-9-]*$', s) and s.count('-') == 3:
        # 使用正则表达式找到最后一个'-'的位置
        parts = re.split(r'(.*)-', s)
        # 替换最后一个'-'的子字符串为ROOM
        # 修改
        return [parts[1], 'ROOM'], parts[2]
    else:
        return [s], None

def transform_list(l_before, link_keywords=['-','ー','の'], back_keywords=['丁目','番地','号','f','F']):
    l_after = []
    temp = []  # 用于临时存储待合并的字符串
    room_nums = []  # 用于存储被替换的数字

    for i, item in enumerate(l_before):
        # 判断是否为数字或者link_keywords且后面跟着数字
        if item.isdigit() or (item in link_keywords and i + 1 < len(l_before) and l_before[i + 1].isdigit()):
            temp.append(item)
        # 判断是否为'JP'后面跟着link_keywords
        elif item in ['jp','JP'] and i + 1 < len(l_before) and l_before[i + 1] in link_keywords:
            temp.append(item)
        # 判断是否为back_keywords且前面跟着数字
        elif item in back_keywords and i - 1 >= 0 and l_before[i - 1].isdigit():
            temp.append(item)
        else:
            if temp:
                # 当遇到非数字和非link_keywords时，合并temp中的元素并添加到l_after
                l_after.append(''.join(temp))
                # 增加操作：存储room_nums
                # 检查当前合并的字符串是否为“ROOM”
                if temp[-1] == 'ROOM':
                    room_nums.append(temp[-2])
                temp = []  # 重置temp
            if item != ',':
                l_after.append(item)

    if temp:
        l_after.append(''.join(temp))

    # 合并日语字符串
    l_after = join_strings(l_after)

    # 分割门牌号
    l_after2 = []
    for each in l_after:
        # split_at_last_dash更改代码以后，此处返回的是一个元祖:[parts[1], 'ROOM'],parts[2]
        sub_list, num = split_at_last_dash(each)
        l_after2 += sub_list
        if num:
            room_nums.append(num)
    l_after = [each for each in l_after2 if each not in ['-', 'ー']]

    output = []
    for item in l_after:
        # 将'市'作为分隔符，将字符串分割
        if '市' in item and '市川' not in item:
            splited = re.split(r'(?<=市)', item)
            output += [each for each in splited if each]
        else:
            if 'Japan' in item:
                output += split_on_japan(item)
            else:
                output.append(item)

    return output, room_nums

def fullwidth_to_halfwidth(s):
    new_string = ""
    for char in s:
        code = ord(char)
        # 检查是否是全角数字或字母
        if 0xFF01 <= code <= 0xFF5E:
            new_string += chr(code - 0xFEE0)
        else:
            new_string += char
    return new_string


def segmentation(text):
    doc = nlp(text)
    l_before = [fullwidth_to_halfwidth(str(each)) for each in doc]
    l_after, room_nums = transform_list(l_before)
    return l_after, room_nums

# dening改动后的代码3--测试完毕
import re
def replace_string(input_str):
    if input_str in ['Japan', 'ROOM']:
        return input_str

    # 匹配数字开头，除了'b'和'f'外的任意字符，不进行转换
    pattern1 =  r'^\d(?![bf])[a-zA-Z]$'
    # 匹配除了'b'和'f'以外的任意字母开头+数字结尾，数字大于等于1, 存疑
    # + 表示很多个
    pattern2 = r'^(?![bf])[a-zA-Z]\d+$'

    # 匹配以'b'开头/结尾,b+数字+f结尾，数字大于等于1小于5，不进行转换
    pattern3 = r'^(b[1-5])$'
    pattern4 = r'^([1-5]b)$'
    pattern5 = r'^b[1-5]f$'
    # 匹配以'f'开头/结尾，数字大于等于1小于等于70，不进行转换
    pattern6 = r'^f(?:[1-9]|[1-6][0-9]|70)$'
    pattern7 = r'^(?:[1-9]|[1-6][0-9]|70)f$'

    # 匹配以'f-'开头，数字大于等于1小于等于70，不进行转换
    # pattern8 = r'^(?![bf])[a-zA-Z]-(?:[1-9]|[1-6][0-9]|70)$'
    # 匹配任意字母开头+-+数字结尾  存疑
    pattern8= r'^[ ]*[a-zA-Z][ ]*-[ ]*([0-9])+$'
    # pattern8= r'^[a-zA-Z]-[1-9]\d*$'

    # 检查是否匹配到不需要转换的模式
    if re.match(pattern1, input_str, re.IGNORECASE) or re.match(pattern2, input_str, re.IGNORECASE) or re.match(
            pattern3, input_str, re.IGNORECASE) \
            or re.match(pattern4, input_str, re.IGNORECASE) or re.match(pattern5, input_str, re.IGNORECASE) or re.match(
        pattern6, input_str, re.IGNORECASE) \
            or re.match(pattern7, input_str, re.IGNORECASE) or re.match(pattern8, input_str, re.IGNORECASE):
        return input_str

    pattern9 = r'(?=.*[a-zA-Z])^[a-zA-Z0-9*]+$'
    #
    # # re.sub replaces the string that matches the pattern with 'RANDOM'
    return re.sub(pattern9, 'RANDOM', input_str)


def convert_list_to_str_for_bert(input_list):
    return [replace_string(each) for each in input_list]

# 函数的功能测试
data=["5-7-11","3E","ff-2","B2","aa56", "s12","768999s","a233","sorvanilladu959"]

convert_list_to_str_for_bert(data)


def get_history_new_processed_df():
    from pyspark.sql import functions as F
    from itertools import combinations
    from pyspark.sql.types import ArrayType, StringType, StructType, StructField
    from pyspark.sql.functions import lit

    # 全量
    query_str = """
    select forter_address__account_id_list_for_the_address as account_id,address from airbot_prod.jp_community
              """
    # 全量数据_包含新数据
    all_df = spark.sql(query_str)

    # 历史数据旧数据-处理增量数据以后与之合并
    history_df = spark.table("airbot_prod.jp_community_tag_dev").select(
        "account_id",
        # "ml_tag_address",
        "shipping_address",
        "jp_zipcode",
        "zipcode_digits",
        # "zipcode_tag",
        "address",
        "parsed_address",
        "mask_messycode",
        # "community_id"
        "room_number",
        "tag",
        "zipcode_tag",
        "final_tag"
    )

    # 找出all_df中存在的account_id，但在history_df中不存在的account_id
    new_account_id_df = all_df.select("account_id").subtract(history_df.select("account_id"))

    # 将new_account_id_df与dfA进行连接，筛选出id在diffIds中的记录
    new_df = all_df.join(new_account_id_df,
                         all_df.account_id == new_account_id_df.account_id,
                         "inner").drop(new_account_id_df.account_id)

    # 定义 UDF 函数，用于将 address 列进行切片处理，期待返回列的值是数组类型
    # 创建新的UDF以适用修改后的segmentation函数
    segmentation_udf = F.udf(segmentation, StructType([
        StructField("parsed_address", ArrayType(StringType()), True),
        StructField("room_number", ArrayType(StringType()), True)
    ]))

    # 定义 UDF 函数，用于将切片后的列表进行转换，期待返回列的值是数组类型
    convert_list_to_str_for_bert_udf = F.udf(lambda x: convert_list_to_str_for_bert(x), ArrayType(StringType()))

    df = new_df.withColumn("segmented_result", segmentation_udf("address")) \
        .select(F.col("account_id"), F.col("address"), F.col("segmented_result.*"))

    # 将空数组（[]）替换为null值
    df = df.withColumn("room_number", F.when(F.size("room_number") == 0, None).otherwise(F.col("room_number")))

    # 转换parsed_address
    df = df.withColumn("mask_messycode", convert_list_to_str_for_bert_udf("parsed_address"))

    print(f"打完标签前的增量数据量：{df.count()}")
    print(f"处理jp_community完地址生成两列数据的schema信息：")
    df.printSchema()
    # df.display()
    df = df.withColumn("tag", F.when(F.array_contains("mask_messycode", "RANDOM"), "RANDOM") \
                       .when(F.array_contains("mask_messycode", "ROOM") & \
                             ~F.array_contains("mask_messycode", "RANDOM"), "ROOM") \
                       .otherwise("CORRECT"))
    # 打标签后的scheme信息

    print(f"打标签后的schema信息：")
    df.printSchema()

    # 去重--得到处理后的数据
    df = df.dropDuplicates(["account_id", "address"])

    # 对齐后合并新数据--此处不需要进行合并，因为增量打完标签形成新的数据结构，将它进一步打完zipcode_tag以后，再与历史数据进行合并
    print(f"打完标签后的增量数据量：{df.count()}")

    return {"final_data": df, "new_account_id_df": new_account_id_df, "history_df": history_df}

# 测试数据
# get_history_new_processed_df().display()


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


# 开始打zipcode_error标签
# add zipcode_error
from pyspark.sql import functions as F

# import re
# Read table data

# 对jp_community打的第一阶段的标签的返回值
# return {"final_data":df,"new_account_id_df":new_account_id_df,"history_df":history_df}
print("------------------------add three tag to increment data------------------------------")

first_tag_dict = get_history_new_processed_df()

# 获取具有type1的zip_code&shipping_address以及处理后的address地址
print("------------------------extract middle table------------------------------")
type1_zipcode_shipping_address_processed_address_df = get_type1_zipcode_shipping_address_processed_address_df(
    first_tag_dict["new_account_id_df"])

# 基于get_all_df()打的tag标签，增加一个zipcode_error标签
new_jp_tag_df = first_tag_dict["final_data"].withColumnRenamed("account_id", "upmid").select(
    "upmid",
    "address",
    "parsed_address",
    "mask_messycode",
    "room_number",
    "tag"
    # "community_id"
).dropDuplicates(["upmid", "tag", "address"])

result_df = new_jp_tag_df.join(
    type1_zipcode_shipping_address_processed_address_df,
    (F.col("account_id") == F.col("upmid")) & (F.col("processed_address") == F.col("address")),
    # 保证shipping_address与address的绝对对应，使用shipping_address较全的table---中间表数据作为底表
    # 但是不合理 少的数据 left join 多的数据 存在多匹配，去重就可以
    how="left"
).dropDuplicates(["upmid", "shipping_address", "address"]).filter(F.col("shipping_address").isNotNull())

# 通过when函数生成新的列zipcode_tag
result_df = result_df \
    .withColumn(
    "zipcode_tag",
    F.when(F.col("zipcode_digits") != 7, "ZIPCODE_ERROR")
) \
    .select(
    "account_id",
    # "ml_tag_address",
    "shipping_address",
    "jp_zipcode",
    "zipcode_digits",
    # "zipcode_tag",
    # "processed_address"
    "address",
    "parsed_address",
    "mask_messycode",
    # "community_id",
    "room_number",
    "tag",
    "zipcode_tag"
)

result_df = result_df.withColumn(
    "final_tag",
    F.expr(
        """
            filter(array(zipcode_tag, tag), x -> x is not null)
        """
    )
)

result_df = result_df.select(
    "account_id",
    # "ml_tag_address",
    "shipping_address",
    "jp_zipcode",
    "zipcode_digits",
    # "zipcode_tag",
    "address",
    "parsed_address",
    "mask_messycode",
    # "community_id"
    "room_number",
    "tag",
    "zipcode_tag",
    "final_tag"
).dropDuplicates(["account_id", "shipping_address", "address"])

# 打zipcode_error标签后合并历史数据
last_df = first_tag_dict["history_df"].union(result_df)

# 将临时视图保存为临时的delta表
last_df.createOrReplaceTempView("jp_tag_v2")

print("------------------------add zipcode_tag------------------------------")
print(
    f"增量join type1、中间表(包含account_id&shipping_address&address)、jp_community_tag_dev 后的数量：{result_df.count()}")
print(f'增量ZIPCODE_ERROR数量：{result_df.filter(F.col("zipcode_digits") != 7).count()}')
print(f'全量ZIPCODE_ERROR数量：{last_df.filter(F.col("zipcode_digits") != 7).count()}')
print(f"(全量)与历史数据合后并的数据量：{last_df.count()}")

# result_df.display()

# 插入中间表
spark.sql(
"""
 insert overwrite table airbot_prod.jp_community_tag_dev_temp
   select a.*,b.community_id from jp_tag_v2 as a inner join airbot_prod.jp_community b on a.account_id=b.forter_address__account_id_list_for_the_address and a.address=b.address
"""
)

# 插入原表_check后
spark.sql(
"""
 insert overwrite table airbot_prod.jp_community_tag_dev select * from airbot_prod.jp_community_tag_dev_temp
"""
)
