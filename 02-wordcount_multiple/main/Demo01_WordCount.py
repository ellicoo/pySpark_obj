from pyspark import SparkConf, SparkContext
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建Spark环境
conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

# 2.读取数据
"""
sc.textFile("../data/word_re.txt")会生成一个RDD，其中每个元素代表文本文件中的一行字符串。
这个RDD可以类比为一个单列的表格或单列的数据集合，每行就是这个数据集合中的一个元素，元素比较粗糙
表现如下：
RDD：
---------------------------
｜"hadoop spark  hive"      ｜
｜"hive hadoop spark spark" ｜
｜"hadoop spark flink"      ｜
｜"hive hadoop spark spark" ｜
｜"flink hbase   hello"     ｜
｜"spark zookeeper flink"   ｜
---------------------------
由于这个rdd比较粗糙，每行数据在RDD中都是一个独立的元素，但没有像表格一样有明确的列名或列的类型信息（即缺乏schema）
这种粗糙性需要使用适当的转换操作（如flatMap）来处理每行数据，以便进行进一步的数据操作和分析。
"""
input_rdd = sc.textFile(get_absolute_path("../data/word_re.txt"))

# 3.处理数据
result_rdd = input_rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)

# 4.输出数据
result_rdd.foreach(lambda x: print(x))

# 5.停止Spark环境
sc.stop()
