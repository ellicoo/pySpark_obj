import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

"""
-------------------------------------------------
   Description :	TODO：演示SparkSQL综合案例
   SourceFile  :	Demo05_SparkSQLExample
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
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可

"""
spark.sql.shuffle.partitions 参数指的是, 在sql计算中, shuffle算子阶段默认的分区数是200个.
对于集群模式来说, 200个默认也算比较合适
如果在local下运行, 200个很多, 在调度上会带来额外的损耗
所以在local下建议修改比较低 比如2\4\10均可
这个参数和Spark RDD中设置并行度的参数 是相互独立的.
"""

spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions","4")\
    .getOrCreate()

# 2.数据输入
input_df = spark.read.text("../data/word.txt")

# 3.数据处理
print("=======1.SQL=======")
#3.1 把DF注册成一张表
input_df.createOrReplaceTempView("t1")
#3.2 编写SQL，SparkSQL中的函数，大部分都和Hive一样
result_df1 = spark.sql("""
select word,count(1) as cnt
from 
    (select 
        explode(split(value,' ')) as word 
    from t1) t2
group by word
""")

print("=======2.DSL=======")
result_df2 = input_df.select(F.split('value',' ').alias("arrs"))\
    .select(F.explode('arrs').alias("word")).groupBy('word').agg(F.count('word').alias('cnt'))


# 4.数据输出
input_df.printSchema()
input_df.show(truncate=False)
result_df1.printSchema()
result_df1.show(truncate=False)
result_df2.printSchema()
result_df2.show(truncate=False)

# 5.关闭SparkContext
spark.stop()
