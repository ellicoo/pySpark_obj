from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：RDD算子练习案例
   SourceFile  :	Demo04_RDDPractise
   Author      :	81196
   Date	       :	2023/9/10
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkContext
conf = SparkConf().setMaster("local[2]").setAppName("AppName")
sc = SparkContext(conf=conf)

# 2.数据输入--天池数据集是阿里巴巴提供的--搜天池数据
# 数据没有去重，一条数据就是一次访问
# 处理策略：按照天分组，一天的count就是一天的访问量
# pv--page view 页面浏览量 --转换-- (天,1)--再reducebykey--再count
# uv--user view 用户浏览量 --转换--(天,user_id)-- 去重 -- 再(天,1)--再reducebykey -- 再count




input_rdd = sc.textFile(get_absolute_path("../data/tianchi_user.csv"))


# 3.数据处理
# 3.1对数据做清晰，转换操作

# 切片
# lines[5][0:10]的含义：
# 0：从0开始（包含）
# 10：到第10个位置结束（不包含）

# 对一行数据的处理
# 把时间先切出来，再把小时拿到
def map_line(line):
    lines = line.split(",")
    return (lines[0], lines[1], lines[2], lines[3], lines[4], lines[5][0:10])

# 文档说有六个字段，切割数据保留长度为6个的
# 但凡对数据进行处理--使用map--但凡能用map的地方都能用mapPartition
filter_rdd = input_rdd.filter(lambda line: len(line.split(",")) == 6).map(lambda line: map_line(line))





# #1.分析
# 预期结果：搜索词	出现次数
#
# #2.实现思路
# step1：读取数据，数据清洗：过滤不合法数据【filter】，将每一行字符串转换成元组【6个元素】【map】
# step2：将所有搜索内容【查询内容】通过分词器将所有搜索词提取出来【每个搜索词作为一个独立元素:flatMap】
# step3：对每个词构建标记，标记出现一次【map】
# step4：按照搜索词进行分组聚合【reduceByKey】，排序取最高的前10【sortBy + take】
#
# 清洗，转换为6个字段的数据
# 使用分词器切割，查询词中会被切割成多个关键词，因此需要使用flatMap算子
# 使用map，转换数据为（单词，1）的形式
# 聚合reduceByKey
# 降低分区
# sortBy
# 3.2统计每天的PV，并按照日期升序排序
result_rdd = (filter_rdd.map(lambda line: line[5])
              # 把日期转换为（日期，1）的形式
              .map(lambda day: (day, 1))
              # 对日期进行reduceByKey操作，统计相同天的PV数量
              .reduceByKey(lambda x, y: x + y)
              # 降低分区操作，避免多个分区，导致最终结果不一致
              .coalesce(1)
              # 升序排序
              .sortByKey(ascending=True))



# #1.分析
# 语义：一个用户搜索某个词，最多搜索了多少次，最少搜索了多少，平均搜索了多少次
# 预期结果：最大搜索次数、最小搜索次数、平均搜索次数
#
# #2.实现思路
# step1：读取数据，数据清洗：过滤不合法数据【filter】，将每一行字符串转换成元组【6个元素】【map】
# step2：将每条数据构建每个KV结构：K【(userid, word)】 V:1
# step3：按照K进行分组聚合，得到每个用户搜索每个词的总次数
# step4：获取所有搜索次数，取最大、最小、平均
#
#
# 清洗，转换为6个字段的数据
# 选择用户ID和查询词这两个字段，当做一次搜素，拼成（（用户ID，查询词），1）
# 聚合reduceByKey
# 取values
# 再针对values的值进行求最大、最小、平均
# 3.3 统计每天的UV，并按照UV个数降序排序
result_rdd2 = (filter_rdd.map(lambda line: (line[5], line[0]))
               # 对（天，user_id）进行去重
               .distinct()
               # 取day这个字段
               .map(lambda line: line[0])
               # 把day转换为（day，1）的形式
               .map(lambda day: (day, 1))
               # 对（day，1）的数据进行reduce聚合操作
               .reduceByKey(lambda x, y: x + y)
               # 降低分区，准备排序
               .coalesce(1)
               # 指定按照uv数量进行排序，且是倒序
               .sortBy(lambda x: x[1], ascending=False))

# 4.数据输出
print("=========1.原始数据=========")
print(filter_rdd.take(2))
print(filter_rdd.count())
print("=========2.PV数据=========")
print(result_rdd.take(5))
print(result_rdd.count())
result_rdd.foreach(lambda x: print(x))
print("=========3.UV数据=========")
print(result_rdd2.take(5))
print(result_rdd2.count())
result_rdd2.foreach(lambda x: print(x))

# 5.关闭SparkContext
sc.stop()
