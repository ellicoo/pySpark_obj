from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：演示filter过滤算子
   SourceFile  :	Demo07_FilterFunction
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
input_rdd = sc.textFile(get_absolute_path("../data/film.txt"))

# 3.数据处理
#方式一
#result_rdd = input_rdd.filter(lambda line:line.split(" ")[1] != '美国')

#方式二
def splitStr(line):
    return line.split(" ")[1] != "美国"
result_rdd = input_rdd.filter(lambda line:splitStr(line))

# 4.数据输出
result_rdd.foreach(lambda x:print(x))

# 5.关闭SparkContext
sc.stop()
