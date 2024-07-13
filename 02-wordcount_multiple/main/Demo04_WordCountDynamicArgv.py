import sys

from pyspark import SparkConf, SparkContext

import os

# 测试动态参数练习--这个文件代码需要在命令行中执行，格式为：

# 这段代码是一个PySpark脚本，通常需要在命令行中运行，而不是在PyCharm中直接运行。PySpark脚本需要与Spark集群进行交互，而不是在本地Python环境中运行。
#
# 以下是在命令行中运行PySpark脚本的一般步骤：
#
# 1.
# 打开终端（命令行界面）。
#
# 2.
# 使用
# `cd`
# 命令导航到包含您的PySpark脚本的目录。例如：
#
# ```bash
# cd / path / to / your / script / directory
# ```
#
# 3.
# 运行您的PySpark脚本，提供正确的命令行参数，如下所示：
#
# ```bash
# spark - submit
# script_name.py
# input_file_path
# output_directory_path
# ```
#
# 其中，`spark - submit`
# 是Spark的命令行工具，`script_name.py`
# 是您的Python脚本的名称，`input_file_path`
# 是要处理的输入文件的路径，`output_directory_path`
# 是Word
# Count结果要保存的输出目录的路径。
#
# 请确保您已经正确安装了Spark，并配置了相关的环境变量，以便在命令行中使用
# `spark - submit`
# 命令。这是因为PySpark需要与Spark集群进行通信，而PyCharm等Python集成开发环境通常不支持直接运行PySpark脚本。
#
# 总之，对于PySpark脚本，建议使用命令行工具
# `spark - submit`
# 来运行，以确保脚本可以正确地与Spark集群交互并执行。


# 也可以在执行代码的操作中，选择edit config...。然后在参数中输入


# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

#1.构建Spark环境
conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

#2.读取数据
# 之前的写法只能处理固定的某个文件，在工作中，数据源应该是由用户指定，而不能是固定的。---引出动态传参

# 需求：读取HDFS分布式文件系统的文件，对文件进行词频统计，且文件由用户输入。
# 由于数据的位置是动态的，所以代码中的数据地址也必须为动态的---松耦合

# python通过sys包的argv方法来实现动态传参数。
#1.导包
# import sys
#2.语法
# sys.argv[0] # 由于python运行代码的特殊性，这个参数固定为python文件的名称，所以不作为第一个参数
# sys.argv[1] # 表示程序真正传递到程序中的第1个参数
# sys.argv[2] # 表示程序真正传递到程序中的第2个参数
# sys.argv[N] # 表示程序真正传递到程序中的第N个参数


# 空格左边为参数1：argv[1] ,空格右边为参数2：argv[2]
# hdfs://node1:8020/spark/wordcount/input/word_re.txt hdfs://node1:8020/spark/wordcount/output1
input_rdd = sc.textFile(sys.argv[1])

#3.处理数据
result_rdd = input_rdd.flatMap(lambda line : line.split(" "))\
    .map(lambda word : (word,1))\
    .reduceByKey(lambda x,y : x + y)

#4.输出数据
result_rdd.saveAsTextFile(sys.argv[2])

#5.停止Spark环境
sc.stop()
