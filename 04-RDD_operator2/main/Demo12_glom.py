from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path
import os

"""
-------------------------------------------------
   Description :	TODO：测试模板是否可用
   SourceFile  :	Demo04_TestTemplate
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

# 创建一个 RDD
rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 2)  # 2个分区

# glom() 函数：将数据以分区排布的效果呈现
# glom() 函数通常在需要对分布在不同分区中的数据执行聚合操作或在数据重组时使用--达到体现数据的分区排布效果

# 在 Spark 中使用 glom() 函数时，不会发生实际的数据移动。相反，
# 它只是在逻辑上将分布在不同分区的数据合并到一个新的 RDD 中，而不涉及数据的物理移动。
# 这是 Spark 的懒加载机制的一部分。
#
# Spark 的懒加载机制指的是在执行转换操作时，它不会立即执行操作，而是构建一个操作的逻辑执行计划。
# 只有在执行一个行动（例如 collect()、count() 等）时，才会实际触发计算和数据移动。
#
# 因此，当您在使用 glom() 函数时，它只是创建了一个新的 RDD，其中的数据是从原始 RDD 的不同分区中提取并合并的，
# 但实际的数据移动只会在执行行动时发生，而不是在转换操作时发生。
# 这种延迟计算和数据移动机制有助于提高 Spark 的性能和效率，因为它允许 Spark 优化执行计划以最小化数据移动和计算开销。

grouped_data = rdd.glom().collect()

# 打印结果
for index, data in enumerate(grouped_data):
    print(f"Partition {index}: {data}")

# glom() 函数将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变。
# 将RDD中每一个分区变成一个数组，并放置在新的RDD中，数组中元素的类型与原分区中元素类型一致

# 假设原始 RDD 有四个分区，每个分区包含 [1, 2], [3, 4], [5, 6], [7, 8] 这样的数据

# rdd = sc.parallelize(data, numSlices=4)
original_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
print('不使用glom+collect时的rdd对象打印')
print(original_rdd)
print('不使用glom但使用glom+collect时的rdd对象打印')
print(original_rdd.collect())

print('使用glom时')
# 使用 glom() ，将rdd中的各分区变成数组
glommed_rdd = original_rdd.glom()
print('使用glom不使用collect时打印rdd对象')
print(glommed_rdd) # PythonRDD[3] at RDD at PythonRDD.scala:53
print('使用glom+collect时打印rdd对象')
print(glommed_rdd.collect()) # [[1, 2], [3, 4], [5, 6], [7, 8]]


# glommed_rdd 中的每个元素都是一个数组，包含了原始 RDD 中一个分区的数据
# 例如，glommed_rdd 的第一个元素就是 [1, 2]

# 停止 SparkContext
sc.stop()
