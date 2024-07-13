from pyspark import SparkContext, SparkConf
import os

# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建Spark环境
conf = SparkConf().setAppName("WordCountYarn").setMaster("local[2]")
sc = SparkContext(conf=conf)

# 2.读取数据源，分区数理论上可以随便指定，仅做参考，实际的分区数不会小于这个值。
# 这种可以随意设置分区的方式，而不影响程序运行，在程序设计中，体现了（健壮）性。
input_rdd = sc.textFile("file:///export/data/word.txt", minPartitions=8)

# 3.查看RDD的数据
numPartitions = input_rdd.getNumPartitions()
result_rdd = input_rdd.glom()
# 4.输出
print(f'文件的最小分区数为：{numPartitions}')
result_rdd.foreach(lambda x: print(x))

# 5.停止Spark环境
sc.stop()
