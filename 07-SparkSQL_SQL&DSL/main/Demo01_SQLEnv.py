from pyspark import SparkContext, SparkConf
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
-------------------------------------------------
   Description :	TODO：SparkSQL环境构建
   SourceFile  :	Demo01_SQLEnv
   Author      :	81196
   Date	       :	2023/9/12
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
SparkSQL是Spark中专门为结构化数据计算的模块，基于SparkCore之上
提供SQL和DSL【代码】开发接口，将SQL或者DSL语句转换为SparkCore程序，实现结构化的数据处理


SparkSQL--spark的一个模块，用于处理大规模结构化数据的分布式SQL计算引擎--限定：结构化的数据
与rdd不同，rdd可以处理结构化，非结构化，半结构化

因为RDD不方便SQL计算，所以推出分布式的DataFram---》DataFrame = RDD + Schema

分布式SQL计算引擎：hive、SparkSQL、presto、impala


SparkSQL使用的数据结构很多：
（1）python：python使用的是2015年推出的DataFram，与python中的pandas中的DataFram数据结构一样
            内部都是存储二维表的数据，只不过SparkSQL的DataFram是分布式的，可以进行分布式计算的
（2）java/Scala：DataSet

SparkSQL前身是Shark框架，shark框架几乎100%的模仿hive，内部配置，优化项，只是引擎换成spark

SparkSQL:
1、SQL融合性：SQL可以无缝集成在代码中，随时用SQL处理数据
2、统一数据访问：一套标准API可读写不同数据源
3、hive兼容：可以使用sparkSQL直接计算并生成hive数据表
4、标准化连接：支持标准化的JDBC\ODBC连接，方便和各种数据库进行数据交互

优点：
1、支持SQL语言
2、性能强
3、可以自动优化
4、API简单
5、兼容hive等

企业大面积使用：离线开发、数仓搭建、科学计算、数据分析
使用rdd存储，则可以用类对象存储，内容存在成员变量中，从而rdd存的是对象集合

SparkSession对象
RDD阶段，程序的执行入口对象是：SparkContext
Spark2.0后,推出了SparkSession对象，作为Spark编码的统一入口对象,通过SparkSession类的sparkContext属性
可以同时初始化出 sc = spark.sparkContext 即sparkContext对象

SparkSession对象可以：
（1）用于SparkSQL编程作为入口对象
（2）用于SparkCore编程，可以通过SparkSession对象中获取到SparkContext对象

所以，我们后续的代码，执行环境入口对象，统一变更为SparkSession



"""

"""
SparkSession 是 Apache Spark 的编程入口，主要用于创建 DataFrame 和执行 Spark SQL 相关的操作。
SparkSession 也是 Spark SQL 的入口点，它用于初始化 Spark 应用程序并配置与 Spark SQL 相关的设置。
因此，可以说它是 Spark SQL 的入口程序。

Spark SQL是用于结构化数据处理的Spark模块。它提供了一种称为DataFrame的编程抽象，是由SchemaRDD发展而来。
不同于SchemaRDD直接继承RDD，DataFrame自己实现了RDD的绝大多数功能。Spark SQL增加了DataFrame（即带有Schema信息的RDD），
使用户可以在Spark SQL中执行SQL语句，数据既可以来自RDD，也可以是Hive、HDFS、Cassandra等外部数据源，还可以是JSON格式的数据。

1.构建SparkSession
SparkSession对象的构建，使用设计模式中的构建者模式（建造者模式）
构建者模式：它用于构建和配置复杂对象。主要思想是将对象的构建和配置分开，以便更灵活地创建不同配置的对象。
builder.build()
builder.getOrCreate()


【Builder是SparkSession类的内部类】
在Python中，内部类通常用于封装与外部类相关的功能。

类的层级关系：
与SparkSession类同级别的模块方法：
def _monkey_patch_RDD(sparkSession):
    # 模块方法中的toDF方法
    def toDF(self, schema=None, sampleRatio=None):
        return sparkSession.createDataFrame(self, schema, sampleRatio)

SparkSession类：
DataFrameReader(OptionUtils)类：
StructType(DataType)类：      
StructField(DataType)类:       


详细：
DataFrameReader(OptionUtils)类：
    # 主要成员方法：
    format(self,source) :return self
    schema(self,schema) : Specifies the input schema. 返回 return self
    option(self,key,value) :return self
    options(self,**options) :return self

    load(self,path=None,format=None,schema=None,**options) :
        Loads data from a data source and returns it as a :class:`DataFrame`

    json(self,path,schema=None....) :  
        Loads JSON files and returns the results as a :class:`DataFrame`.

    table(self,tableName): 
        Returns the specified table as a :class:`DataFrame`.

    parquet(self,*paths,**options)： 
        Loads Parquet files, returning the result as a :class:`DataFrame`.

    text(self,paths, ....)：
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.

    csv(self,path,schema=None...) ：
        Loads a CSV file and returns the result as a  :class:`DataFrame`.

    orc(self,path,...) ：
        Loads ORC files, returning the result as a :class:`DataFrame`.

    jdbc(self,url,table,clumn=None,....)：
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.



SparkSession类：
    # 构造函数
    def __init__(self, sparkContext, jsparkSession=None)
    # 一、局部内部类
    Builder类：
        # 局部内部类的方法：
        def config(self, key=None, value=None, conf=None):
        def master(self, master): return self.config("spark.master", master)
        def appName(self, name):return self.config("spark.app.name", name)
        def getOrCreate(self): return session = SparkSession(SparkContext.getOrCreate(SparkConf()))
        ......

    # 二、SparkSession类的成员函数

    #（1）与sparkContext相关的方法，作为SparkCore程序的入口操作rdd：
    def sparkContext(self):
        Returns the underlying :class:`SparkContext`.

    #（2）创建DataFrame数据集对象：
    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.
        #最终还是返回DataFrame类的对象

    def range(self, start, end=None, step=1, numPartitions=None): 返回DataFrame对象
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

    def read(self):
        Returns a :class:`DataFrameReader` that can be used to read data in as a :class:`DataFrame`.
        return DataFrameReader(self._wrapped)

        【说明】
        其中session = None 或者 session = SparkSession(sc) 
        而 sc = SparkContext.getOrCreate(sparkConf) ，其中 sparkConf = SparkConf()

        对比SparkConf对象构建SparkContext对象：
        conf = SparkConf().setAppName("test").setMaster("local[*]")
        sc = SparkContext(conf=conf)


用户可以使用SparkSession.builder来创建和配置SparkSession实例，
而不必了解内部的实现细节。这有助于提供更清晰和简洁的API，并隐藏了构建过程中的一些细节。

"""

# 1.构建SparkSession
# SparkSession对象的构建，使用设计模式中的构建者模式（建造者模式）
# builder.build()
# builder.getOrCreate()
# 通过内部类构建者builder的getOrCreate()方法返回外部类SparkSession的对象
# flink中的table类通过内部类createtable的成员方法的build()创建对象
# flink中的schema信息也是这样

spark = SparkSession.builder.master("local[2]").appName("PparkSQL").getOrCreate()

# 2.数据输入
# df表示一个DataFrame，本质上就是一张表
df1 = spark.createDataFrame(data=[(1001, 'zhangsan', 19), (1002, 'lisi', 20), (1003, 'wangwu', 21)])

df2 = spark.createDataFrame(data=[(1001, 'zhangsan', 19), (1002, 'lisi', 20), (1003, 'wangwu', 21)],
                            schema=['id', 'name', 'age'])

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)])

df3 = spark.createDataFrame(data=[(1001, 'zhangsan', 19), (1002, 'lisi', 20), (1003, 'wangwu', 21)], schema=schema)

# toDF
schema1 = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)])
input_rdd = spark.sparkContext.textFile("../data/people.txt")
df4 = input_rdd.toDF(schema=schema1)

# spark.read.数据源
df5 = spark.read.json(path="../data/people.json")

# 3.数据处理

# 4.数据输出
# 4.1打印df的Schema信息
df1.printSchema()
# 4.2打印df的表数据
df1.show()

df2.printSchema()
df2.show()

df3.printSchema()
df3.show()

input_rdd.foreach(lambda x:print(x))

df4.printSchema()
df4.show()

df5.printSchema()
df5.show()

# 5.关闭SparkSession
