from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：结构化流实现词频统计案例
   SourceFile  :	Demo01_WordCount
   Author      :	81196
   Date	       :	2023/9/19
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
    .appName("StructuredStreamingWordCount") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

"""
如果操作一个df的行时，需要将第一个df的行数据带入第二个df中进行计算，借助第二个df的计算返回一个值作为第一个df的新列值时候需要
"""


def get_source_community_address_like_count_df(source_df, specify_cpmmunity_df):
    # 获取第一个DataFrame
    df1 = source_df.withColumn("header_2_str", F.concat(F.split(F.col("address"), " ")[0], F.lit(" "),
                                                        F.split(F.col("address"), " ")[1]))

    # .dropDuplicates(["community_id","header_2_str"])避免collect导致的driver进程内存溢满
    df1_proceeded_list = df1.select("community_id", "header_2_str").dropDuplicates(
        ["community_id", "header_2_str"]).collect()
    # df1_pro_list=list(set(df1_proceeded_list)) # 已经在进入driver进程的时候就进行了去重

    # 进行5000多次函数调用
    print(f"数据源需要处理的header_2_str个数:{len(df1_proceeded_list)}")

    # 获取第二个DataFrame，并添加header_2_str列
    df2 = specify_cpmmunity_df.filter(F.col("upmid").isNotNull())

    # 结果列表
    result_list = []

    def keyword_count(row):
        keyword = row.header_2_str
        community_id = row.community_id

        # 在本表中进行关键字查询，并返回包含该关键字的行数
        count = df2.filter((F.col("community_id") == community_id) & (F.col("address").like(f"%{keyword}%"))).count()

        # 将结果作为新列返回
        return (community_id, keyword, count)

    for row in df1_proceeded_list:
        result_list.append(keyword_count(row))

    mid_df = spark.createDataFrame(result_list, ["community_id", "header_2_str", "address_like_count"])

    # 与数据源进行连接

    result_df = df1.join(mid_df, ["community_id", "header_2_str"], "left").dropDuplicates(
        ["upmid", "community_id", "address"]).select("upmid", "community_id", "address", "header_2_str",
                                                     "address_like_count")
    return result_df

# result_df=result_df.select("upmid","community_id","address","header_2_str","address_like_count")
# result_df.write.mode('overwrite').parquet('s3://ngap--customer-data-science--prod--us-east-1/airbot/prod/dening/temp_human_data_processed_backup')
