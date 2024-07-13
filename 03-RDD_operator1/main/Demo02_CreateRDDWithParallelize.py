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

# 2.读取数据（使用并行化集合的方式）
input_rdd = sc.parallelize([1, 2, 3, 4, 5, 6], numSlices=2)

# 3.查看RDD
numPartitions = input_rdd.getNumPartitions()

# 4.输出rdd的分区数
print(f'分区的数量为：{numPartitions}')

# 5.停止环境
sc.stop()
