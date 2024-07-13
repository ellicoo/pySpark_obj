from pyspark import SparkContext, SparkConf
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：DSL风格演示
   SourceFile  :	Demo03_DSQLPractise
   Author      :	81196
   Date	       :	2023/9/12
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkSession
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# 2.数据输入
input_rdd = spark.sparkContext.parallelize([(1001, 'zhangsan', 18), (1002, 'lisi', 28), (1003, 'wangwu', 38)])

# 3.数据处理
input_df = input_rdd.toDF(['id', 'name', 'age'])
# 3.1查看表的Schema信息
input_df.printSchema()
# 3.2查看数据（默认查看前20条数据，长度不超过20个字符）
input_df.show()
# 3.3 查看id和name
# 字符串的访问方式--常用这种方式
result_df = input_df.select('id', 'name')
# 列表的访问方式
result_df2 = input_df.select(['id', 'name'])
# Column对象（Column：列）
# Row：行，一行数据就是一个Row对象
# Column：列，一列数据就是一个Column对象
# Schema：所有列的集合
result_df3 = input_df.select(input_df['id'], input_df['name'])

# 3.4 过滤
result_df4 = input_df.where("age > 20")
result_df5 = input_df.where(input_df['age'] > 20)
result_df6 = input_df.where(input_df['age'] == 28)
# 3.5 分组，后面一定是聚合。
# 聚合方式一，分组后直接调用聚合函数(只能跟一种聚合)
result_df7 = input_df.groupby('age').count()
# 聚合方式二，分组后调用agg(可以跟多个聚合)
"""
pyspark.sql.functions 模块包含了许多用于在Spark中进行数据处理和转换的函数。
这些函数可以用于操作和处理Spark DataFrame 中的列。以下是一些常见的功能：

(1)数学和统计函数：

col(): 引用列。
lit(): 创建一个常量列。
abs(): 绝对值。
sqrt(): 平方根。
sum(): 求和。
avg(): 平均值。
min(): 最小值。
max(): 最大值。

(2)字符串函数：

concat(): 连接字符串。
substring(): 获取子字符串。
length(): 字符串长度。
trim(): 去除字符串两侧的空格。
lower(), upper(): 转换为小写或大写。

(3)日期和时间函数：

current_date(), current_timestamp(): 获取当前日期或时间戳。
datediff(): 计算日期之间的天数差异。
date_add(), date_sub(): 增加或减少日期。
year(), month(), day(): 提取年、月、日。

(4)条件表达式和空值处理：

when(), otherwise(): 条件表达式。
coalesce(): 返回第一个非空值。
isnan(), isnull(): 检查是否为 NaN 或 NULL。

(5)数组和集合函数：
array(): 创建数组。
explode(): 展开数组中的元素。
size(): 数组大小。
array_contains(): 检查数组是否包含特定值。

(6)窗口函数：
window(): 定义窗口规范。
row_number(), rank(), dense_rank(): 窗口排名函数。

(7)其他功能：
rand(), randn(): 生成随机数。
first(), last(): 获取第一个或最后一个元素。
lead(), lag(): 获取下一个或上一个元素。

这只是 pyspark.sql.functions 中一小部分功能的介绍。
你可以根据具体的需求查看Spark文档以获取更详细的信息和用法。
这些函数在Spark SQL和DataFrame API中都可以使用。

"""
# Column对象记录一列数据并包含列的信息，Column对象有更名列名操作alias
result_df8 = input_df.groupBy('age').agg(F.count('age').alias('cnt'), F.max('id'))

# 使用面向对象的方式
result_df9 = input_df.select(input_df.id, input_df.age)

# 4.数据输出
# result_df.printSchema()
# result_df.show()
# result_df2.printSchema()
# result_df2.show()
# result_df3.printSchema()
# result_df3.show()
# result_df4.printSchema()
# result_df4.show()
# result_df5.printSchema()
# result_df5.show()
# result_df6.printSchema()
# result_df6.show()
# result_df7.printSchema()
# result_df7.show()
# result_df8.printSchema()
# result_df8.show()
result_df9.printSchema()
result_df9.show()
# 5.关闭SparkContext
spark.stop()
