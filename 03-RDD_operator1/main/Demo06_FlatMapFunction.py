import re

from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo06_FlatMapFunction
   Author      :	81196
   Date	       :	2023/9/7
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

# 2.数据输入
lists = ['光辉岁月/海阔天空/情人','十年/爱情转移/孤勇者','第一次爱的人/大眠/当你','撕夜/天黑/他一定很爱你']
input_rdd = sc.parallelize(lists)

# 3.数据处理
result_rdd = input_rdd.flatMap(lambda line:re.split('/',line))

# 4.数据输出
input_rdd.glom().foreach(lambda x:print(x))
print("--------Ctrl + D：快速复制一行---------")
result_rdd.glom().foreach(lambda x:print(x))
print("--------Ctrl + Shift + 方向键：快速向上/下移动---------")
result_rdd.foreach(lambda x:print(x))
# 5.关闭SparkContext
sc.stop()
