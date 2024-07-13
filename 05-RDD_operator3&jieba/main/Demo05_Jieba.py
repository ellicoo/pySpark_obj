import jieba
from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO：演示jieba分词器
   SourceFile  :	Demo05_Jieba
   Author      :	81196
   Date	       :	2023/9/10
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

# 定义一个字符串
line = '我来到北京清华大学'

# TODO：全模式分词--将句子中所有可以组成词的词语都扫描出来, 速度非常快，但可能会出现歧义
seg_list = jieba.cut(line, cut_all=True)
print(",".join(seg_list))

# TODO: 精确模式--将句子最精确地按照语义切开，适合文本分析，提取语义中存在的每个词
seg_list_2 = jieba.cut(line, cut_all=False)
print(",".join(seg_list_2))

# TODO: 搜索引擎模式--在精确模式的基础上，对长词再次切分，适合用于搜索引擎分词
seg_list_3 = jieba.cut_for_search(line)
print(",".join(seg_list_3))


# 5.关闭SparkContext
sc.stop()
