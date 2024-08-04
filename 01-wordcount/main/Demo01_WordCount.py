# 1.构建SparkContext环境
from pyspark import SparkContext, SparkConf
from my_utils.get_local_file_system_absolute_path import get_absolute_path

import os

# 配置SPARK_HOME的路径
os.environ['SPARK_HOME'] = '/export/server/spark'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
# 配置base环境Python解析器的路径
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
class PipelinedRDD(RDD):
class RDD(object):
    # 构造函数略
    # 成员函数
    # （1）将rdd进行持久化存储--事先认为安全，所以不存储血缘关系，只存储一个rdd的计算结果
    def checkpoint(self):
    # （2）底层基本算子
    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        return PipelinedRDD(self, f, preservesPartitioning)
    def mapPartitions(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    # （3）map算子
    def map(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)
    # （4）flatMap算子
    def flatMap(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    # （5）getNumPartitions算子
    def getNumPartitions(self):
        返回一个非rdd的值
    # （6）过滤rdd数据集中的元素
    def filter(self, f):
        return self.mapPartitions(func, True)
    # （7）给rdd去重
    def distinct(self, numPartitions=None):
        return self.map(lambda x: (x, None)) \
                   .reduceByKey(lambda x, _: x, numPartitions) \
                   .map(lambda x: x[0])
    # （8）合并rdd--会被SparkContext程序入口类的union方法调用
    def union(self, other):
        return rdd

    # （9）基本分区
    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        return rdd

    # （10）sortByKey--对rdd根据键值对中的key进行排序
    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        return self.partitionBy(numPartitions, rangePartitioner).mapPartitions(sortPartition, True)

    # （11）sortBy
    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        return self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()

    # （12）glom--将RDD的数据，加上嵌套，这个嵌套按照分区来进行的
    def glom(self):
        return self.mapPartitions(func)
    # （13） groupBy
    def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)

    #（14）foreach & foreachPartition
    def foreachPartition(self, f):
        self.mapPartitions(func).count()  # Force evaluation
    def foreach(self, f):
        self.mapPartitions(processPartition).count()
     def count(self):
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()
    def sum(self):
        return self.mapPartitions(lambda x: [sum(x)]).fold(0, operator.add)

    #（15）collect算子--将rdd分布式对象中的每个分区数据，都发送到Driver中，形成一个python list 对象
    def collect(self):
        return list(_load_from_socket(sock_info, self._jrdd_deserializer))
    #（16）groupByKey & reduceByKey & aggregateByKey & foldByKey
    def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
        return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None,
                       partitionFunc=portable_hash):
        return self.combineByKey(
            lambda v: seqFunc(createZero(), v), seqFunc, combFunc, numPartitions, partitionFunc)

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        return self.combineByKey(lambda v: func(createZero(), v), func, func, numPartitions,
                                 partitionFunc)
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None, partitionFunc=portable_hash):
        return shuffled.mapPartitions(_mergeCombiners, preservesPartitioning=True)

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash)
        return shuffled.mapPartitions(groupByKey, True).mapValues(ResultIterable)

    #（17）缓存和持久化rdd
    def checkpoint(self):
    def cache(self):
    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):


# 初始化SparkContext类的成员变量用
class SparkConf(object):

程序入口类的层级关系：
class SparkContext：
     # （1）构造函数--通过conf对象进行参数的初始化或者直接给定master, appName值
     def __init__(self, master=None, appName=None, conf=None....):

     # （2）成员函数--在Python中，方法（也称为函数）是可以嵌套的
    def parallelize(self, c, numSlices=None):
        # 方法嵌套
        def createRDDServer():
        .....
    # （3）读取文件创建RDD类对象的函数
    def textFile(self, name, minPartitions=None, use_unicode=True):
        return RDD(self._jsc.textFile(name, minPartitions), self,
                   UTF8Deserializer(use_unicode))
    # （4）读取小文件创建RDD类对象的函数
    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        return RDD(self._jsc.wholeTextFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)))

    # （5）合并两个rdd类对象的函数
    def union(self, rdds):
        return RDD(self._jsc.union(jrdds), self, rdds[0]._jrdd_deserializer)

    # （6）将变量设置广播变量的函数--变量名.broadcast--将本地小数据发送给
    def broadcast(self, value):
        Broadcast a read-only variable to the cluster, returning a :class:`Broadcast`
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.
       return Broadcast(self, value, self._pickled_broadcast_vars)

    # （7）定义分布式累加器函数
    def accumulator(self, value, accum_param=None):
        Create an :class:`Accumulator` with the given initial value, using a given
        :class:`AccumulatorParam` helper object to define how to add values of the
        data type if provided. Default AccumulatorParams are used for integers
        and floating-point numbers if you do not provide one. For other types,
        a custom AccumulatorParam can be used

    # （8）设置缓存检查点，也就是缓存rdd到本地文件系统或者hdfs
    def setCheckpointDir(self, dirName):
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be an HDFS path if running on a cluster.

    # （9）关闭SparkContext所构建的环境
    def stop(self): 无返回值

SparkContext类对象通常取名sc--通过sc对象使用上面的8个函数 ，第1个函数是初始化sc对象用的





spark的核心是由rdd实现的：即rdd是spark最核心的抽象对象
每个action算子确定一条DAG任务执行链条，每遇到一个action，它就触发它之前的所有操作

一、DAG：有向无环图，执行图
所以：一个action会产生一个job(application中的一个子任务)，每个job有自己的DAG执行图
1个action = 1个DAG = 1个job = 1个application的子任务
如果一整个代码中写了3个action，执行起来就会产生一个application，而application中产生3个job，每个job有各自的DAG

算子：1）转换类算子，2）action算子
区别：
      转换算子返回值100%是rdd，acton算子100%不是rdd
      转换类算子是懒加载的，只有遇到action才会执行，action就是转换类算子处理链条的开关

二、DAG的宽窄依赖和阶段划分：
1）阶段划分：spark会根据DAG，按照宽窄依赖，划分不同的DAG阶段(stage)
   划分依据：从后向前，遇到宽依赖，就划分出一个阶段，称之为stage

2）依赖类型：
窄依赖：父rdd的一个分区，【全部】将数据发给子rdd的一个分区
宽依赖(shuffle依赖)：父rdd的一个分区，将数据发给子rdd的多个分区--简单判断：看父rdd有没有分叉出数据给子rdd的多个分区
宽依赖别名：shuffull
宽依赖通常发生在groupByKey、reduceByKey、join等需要数据重组或混合的操作上。

在stage内部，一定都是：窄依赖(阶段内数据比较规整)

三、内存迭代计算：
spark中task是线程概念，并行的优先级大于内存计算管道PipLine。把所有的计算任务(线程)都放一个executor进程中，固然避免走网络导致的IO压力，
但这样就没有并行属性了。想要全内存计算，直接搞local模式，不需要yarn。

四、spark的并行度：
定义：同一时间内，有多少个task在同时运行
并行度：并行能力的设置，不是设定rdd分区，而是设定并行的数量，因为设置了并行数量，rdd就被构造了一样的分区数量
        先有3个并行度，才有3个分区划分，rdd的一个分区只会被一个task或者说一个并行所处理，一个task可以处理多个rdd(并行的)中的一个分区
        一个executor中有多个task线程

        在多个task线程并行条件下：
        横向看：一个task线程可以处理多个rdd中的一个分区
        竖向看：一个rdd有多个分区，需要被多个task线程处理，而一个task线程只能处理一个rdd中的一个分区

比如设置并行度6，就是要6个task并行在跑，在有6个task并行的前提下，rdd的分区就被划分成6个分区了


如何设置并行度：
（1）代码中设置: 
            conf=SparkConf()
            conf.set("spark.default.parallelism, "100")
（2）客户端设置：bin/spark-submit --conf "spark.default.parallelism = 100"
（3）配置文件中设置： conf/spark-defaults.conf中设置
（4）默认（1，但是不会全部以1来跑，多数时候基于读取文件的分片数量来作为默认并行度）

全局并行度配置的参数：
spark.default.parallelism

全局并行度是推荐设置，不要单独针对rdd改分区，可能会影响内存迭代管道的构建，或者会产生额外的shuffle

五、集群任务中如何规划并行度(只看集群总CPU核心数)
结论：设置为cpu总核心的2～10倍，比如集群可用cpu核心是100个，建议并行度设置为200～1000
100核心如果只设置100并行，某个task比较简单就会先执行完，导致task所在cup核心空闲


七、Driver(领导)的两大组件
（1）DAG调度器：
    工作内容：将逻辑的DAG图进行处理，最终得到逻辑上的Task划分

   【分析】：
    先根据DAG分解task任务，再看给几个executor，推荐100个核心，就给100个executor，一个服务器只开一个executor，如果一个
    服务器开多个executor进程，运行着各自task线程，线程之间的交互走网络(本服务器就是本地环回网络)而不是内存，因为进程隔离，
    一台服务器开一个executor进程即可，再在executor进程中执行多个task线程，这样task线程处理的多个rdd之间的通讯就走内存而不是网络

（2）Task调度器
    工作内容：基于DAG Schedule产生出，来规划这些逻辑的task，应该在哪些物理的executor上运行，以及监控管理他们的运行


简述：一台16核的服务器开1个executor进程，分配16个task线程任务，每次处理(rdd链条链上)多个rdd中的一个分区数据
    总之，只要确保task线程能够榨干cpu就行


总结：spark程序层级关系处理：
1、一个Spark环境（一个local或者yarn环境）可以运行多个application
2、一个代码运行起来，会成为一个application
3、application内部可以有多个job
4、每个job都由rdd的action算子触发，并且每个job有自己的DAG执行图
5、一个job的DAG执行图，会基于宽窄依赖划分成不同的阶段
6、不同阶段内，基于分区数量，形成多个并行的内存迭代管道pipline
7、每个内存迭代管道pipline形成一个task（task是由DAG调度器划分的，将job内划分出具体的task任务，
                                    一个job被划分出来的task在逻辑上称为这个job的taskset）



【注意1】但是spark的性能保证，是在保证并行度的前提下，再尽量去走内存迭代管道pipline。

spark默认受到全局并行度的限制，除了个别算子有特殊分区要求的情况，大部分算子都会遵循全局并行度的要求，来划分自己的分区数
如果全局并行度是3，其实大部分算子分区都是3

【注意2】spark我们一般推荐只设置全局并行度，不要再在算子上设置并行度，改不好就变成shuffle，比如rdd3是3个分区，
        强行改成5个分区后，必然导致父亲rdd2有分叉从而走shuffle，从而产生一个新的阶段，使得rdd3前面的阶段的pipline管道变短，
        计算的内容变少，性能下降
        所以，一改rdd分区，就会影响内存计算管道pipline
        除了一些排序算子外，计算算子就让他默认分区即可即parallelize
        或者textFile时设置的全局分区数，即并行度。

【面试1】：spark是怎么做内存计算的？DAG的作用？stage阶段划分的作用？
【根据DAG先分解任务，再看条件】
1、spark会产生DAG图 -- DAG的作用：就是为了内存计算
2、DAG图基于分区和宽窄依赖关系划分阶段 -- 作用：为了构建内存计算，好从逻辑上构建task任务，变为实际行动，任务规划后再看你实际给几个executor
3、一个阶段的内部都是窄依赖，窄依赖内，如果形成前后的1：1的分区对应关系(rdd不改分区时)，就可以产生许多内存迭代计算的管道
4、这些内存迭代计算的管道，就是一个个具体的执行task-- 1个内存计算管道pipline = 1个task(内部有多个窄依赖的rdd)  = 1个线程
5、一个task是一个具体的线程，任务跑在一个线程内，就是做内存计算了

【面试2】：spark为什么比mapreduce快？
1、spark有丰富的算子，mapreduce只有2个，复杂任务时mapreduce要进行串联，多个mr串联会通过磁盘交互数据 
2、spark是内存迭代，mr是磁盘迭代。spark的rdd算子之间形成DAG基于依赖划分阶段后，在阶段内形成内存迭代管道pipline，暂时不落盘
    但mr的map和reduce之间的交互依旧是通过磁盘来交互的，中间有shuffle(分区、排序、规约、分组)

说人话就是，spark算子多，一个程序搞定，同时rdd算子交互和计算上，可以尽量多的执行内存迭代计算而不是磁盘迭代
但是阶段之间的宽窄依赖，大部分还是网络交互的，因为shuffle

MapReduce执行流程： 
读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘
（1）map：
每个 Map 任务都有一个内存缓冲区(缓冲区大小 100MB )
MapTask任务处理后产生的中间结果：写入内存缓冲区中。如果写人的数据达到内存缓冲的阈值( 80MB )，会启动一个线程将内存中的溢出数据写入磁盘

（2）map和reduce的关系(桥梁)：
Map 阶段处理的数据如何传递给 Reduce 阶段 ？：靠shuffle
Shuffle 会将 MapTask 输出的处理结果数据分发给 ReduceTask ，并在分发的过程中，对数据按 key 进行分区和排序。

（3）reduce：
ReduceTask 的数据流是<key, {value list}>形式，用户可以自定义 reduce()方法进行逻辑处理，最终以<key, value>的形式输出。
MapReduce 框架会自动把 ReduceTask 生成的<key, value>传入 OutputFormat 的 write 方法，实现文件的写入操作。

计算复杂任务时：【读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘】--（磁盘IO）--> 【读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘】


spark的Catalyst 是 Spark SQL 中的查询优化器：
Catalyst 优化器会优化传递调用的情况，通过合并操作和优化执行计划来提高性能。然而，对于重复调用的情况，
Catalyst 优化器无法自动缓存中间结果。因此，如果一个中间结果在多个地方被重复使用，
手动使用 persist(StorageLevel.MEMORY_AND_DISK) 或其他适当的缓存策略是必要的。

Catalyst 优化器的作用：
Catalyst 优化器主要负责以下几方面的优化：

1.谓词下推：将过滤条件尽量推到数据源读取时执行，减少数据量。
2.投影修剪：只读取和处理必要的列。
3.操作合并：将多个连续的操作合并为一个操作。
4.重排序：根据代价模型重排序操作，优化查询计划。
然而，Catalyst 优化器并不会自动检测到数据帧被多次使用的情况并自动进行缓存。因此，对于需要重复使用的中间结果，手动缓存是提高性能的关键步骤
"""
# AppName:应用名称
# Master:Spark的运行模式，这里指定以Local模式运行

# 1、SparkConfig类：1）setAppName方法，返回self，2）setMaster，返回self--因为返回self，可链式操作
# 2、SparkContext类：包装

# 源代码中：
#     When you create a new SparkContext, at least the master and app name should
#     be set, either through the named parameters here or through `conf`.

# 要创建SparkContext类的对象至少需要设置两个参数Master和AppName

# SparkContext 类是 Apache Spark 中的主要入口点，用于与 Spark 集群通信并管理 Spark 应用程序的执行。
# 当创建 SparkContext 对象时，通常会将一个 SparkConf 对象传递给它，以指定应用程序的配置。
# 这样，SparkContext 将根据您在 SparkConf 中设置的选项来初始化 Spark 应用程序。

conf = SparkConf().setAppName("WordCount").setMaster("local[2]")
# 一、可以手动选择输入参数，也可以使用SparkConf类对象及其操作来初始化SparkContext类对象
# sc = SparkContext(master="local[2]", appName="WordCount")

# 这种方式是完全有效的，而且更为简洁。实际上，这是更常见的用法，特别是对于一些常用的配置选项。
# 然而，使用 SparkConf 来配置 SparkContext 应用程序具有以下一些好处：
#
# （1）代码可读性和维护性： 在配置选项比较多、复杂的情况下，将所有配置选项集中到一个 SparkConf 对象中，
# 可以提高代码的可读性和维护性。这样，您可以一目了然地看到应用程序的所有配置，而不必在 SparkContext 构造函数中嵌套大量的参数。
#
# （2）灵活性： 使用 SparkConf 可以在创建 SparkContext 前灵活地设置和修改配置选项。这在某些情况下非常有用，
# 例如，根据应用程序的不同运行模式（本地模式、集群模式等）来动态配置选项。
#
# （3）可重用性： 您可以创建一个通用的 SparkConf 对象，包含了大多数或所有应用程序可能需要的配置选项，
# 然后在不同的应用程序中重复使用这个对象，而只需调整部分参数。

# 二、使用 SparkConf 来配置 Spark 应用程序，即初始化SparkContext类对象

# 1. 构建spark环境--使用SparkContext类帮我们初始化环境
sc = SparkContext(conf=conf)

# textFile 方法是 SparkContext 类的一个方法，它用于根据外部数据源（例如文本文件）切割映射成一个RDD对象并返回，
# 也可以说根据外部数据映射，创建一个映射的RDD类对象并返回。

# 2.数据输入--读取外部数据源--使用SparkContext类的textFile方法将外部数据映射成rdd类的对象并返回该rdd对象。这个
# rdd对象的元素是多行的数据组成的分区，存在行符的
input_rdd = sc.textFile(get_absolute_path("../data/word.txt"), 100)
# 也可以使用本地集合转分布式的方式创建并行的rdd集合

# 3.数据处理--使用合适的API处理数据
# 将存在换行符号的rdd对象经过flatMap，把换行符拿掉，或者处理掉包含嵌套列表的结果--实现扁平化处理
result_rdd = input_rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)  # reduceByKey 中接收的参数是：具有相同键的两个值。即 x 和 y 分别表示具有相同键的两个值
# map函数会返回一个元祖对象--做成键值对对象

# 4.数据输出
result_rdd.foreach(lambda x: print(x))
print('分区数：', input_rdd.getNumPartitions())
# textFile中最小分区数是个参考值，spark有自己的判断，本例中忽略了最小分区100的条件
# 给的太大，或者太小都不会影响

# 5.停止任务
sc.stop()
