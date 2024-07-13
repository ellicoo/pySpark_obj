from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示Join算子
   SourceFile  :	Demo08_JoinFunction
   Author      :	81196
   Date	       :	2023/9/8
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
rdd_singer_age = sc.parallelize([("周杰伦", 43), ("陈奕迅", 47), ("蔡依林", 41), ("林子祥", 74), ("陈升", 63)], numSlices= 2)
rdd_singer_music = sc.parallelize([("周杰伦", "青花瓷"), ("陈奕迅", "孤勇者"), ("beyond", "海阔天空"), ("林子祥", "男儿当自强"), ("动力火车", "当")], numSlices=2)

# 3.数据处理
print("==========1.join算子=========")
join_rdd = rdd_singer_age.join(rdd_singer_music)
join_rdd.foreach(lambda x:print(x))
print("==========2.left join算子=========")
left_join_rdd = rdd_singer_age.leftOuterJoin(rdd_singer_music)
left_join_rdd.foreach(lambda x:print(x))
print("==========3.right join算子=========")
right_join_rdd = rdd_singer_age.rightOuterJoin(rdd_singer_music)
right_join_rdd.foreach(lambda x:print(x))
print("==========4.full join算子=========")
full_join_rdd = rdd_singer_age.fullOuterJoin(rdd_singer_music)
full_join_rdd.foreach(lambda x:print(x))

# 4.数据输出



# 5.关闭SparkContext
sc.stop()
