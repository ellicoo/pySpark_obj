from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：读写CSV文件案例
   SourceFile  :	Demo01_ReadCsv
   Author      :	81196
   Date	       :	2023/9/14
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
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()



# 2.数据输入
#方式一：spark.read.csv方式，默认是(,)分隔符，可以指定固定的分隔符
input_df = spark.read.csv("../data/people.csv",
                          schema="name string,age int,job string",
                          sep=";",
                          header=True,
                          inferSchema=True)

#方式二，spark.read.format(“csv”)
input_df2 = spark.read.format('csv')\
    .option('sep',';')\
    .option('header','True')\
    .option('inferSchema','True')\
    .load('../data/people.csv')



# 3.数据处理
#数据写出,方式一
#df.write.csv()
input_df.write.mode('overwrite').csv(path='../data/output/csv01',header=True)
#df.write.format("csv")
input_df2.write.format("csv").mode("overwrite").option("header","True").save("../data/output/csv02")

# 4.数据输出
# input_df.printSchema()
# input_df.show()
# input_df2.printSchema()
# input_df2.show()
print("==save csv文件==")

# 5.关闭SparkContext
spark.stop()
