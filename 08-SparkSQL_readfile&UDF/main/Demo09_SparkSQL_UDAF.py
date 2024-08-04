# coding:utf8
import os

from pandas import Series
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import pyspark.sql.functions as F
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：SparkSQL模版
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
DataFrame的组成：
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息

"""

if __name__ == '__main__':
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
    spark.sql("show databases").show()
    spark.sql("use insurance_dw").show()
    spark.sql("show tables").show()
    spark.sql("select * from prem_src4").show()

    # 3.数据处理
    # 创建并初始化有效保单数lx列
    # if与case when的区别：
    # 对于简单的两个条件逻辑，CASE WHEN 和 IF 可以互相替换，但是当超过两个条件时，只能case when，
    # 比如对于多个条件的情况，CASE WHEN 更加适合，因为它可以处理多个WHEN子句，而IF函数只能处理单个条件
    spark.sql("""
        create or replace view prem_src5_1 as
           select 
           *,
           if(policy_year = 1, 1, null) as lx_d,
           if(policy_year = 1, qx_d, null) as dx_d,
           if(policy_year = 1, qx_ci, null) as dx_ci
        from prem_src3;
        """)


    # 自定义处理函数
    @F.pandas_udf(returnType=FloatType())
    def spark_udaf_func(lx_d: Series, dx_d: Series, dx_ci: Series) -> float:
        # 计算逻辑：当第一保单年度时，即policy_year=1时，lx_d=1, dx_d=qx_d, dx_ci=qx_ci
        # 在第2及后续保单年度时，即policy_year>=2时
        # lx_d=上一年度的lx_d-上一年度的dx_d-上一年度的dx_ci
        # dx_d=lx_d*qx_d
        # dx_ci=lx_d*qx_ci
        tmp_lx_d = 0
        tmp_dx_d = 0
        tmp_dx_ci = 0
        for i in range(len(lx_d)):
            if i == 0:
                tmp_lx_d = float(lx_d[i])
            else:
                tmp_lx = tmp_lx * (1 - float(qx[i - 1]))
        return tmp_lx


    spark.udf.register("spark_udaf", spark_udaf_func)

    spark.sql("""
        create table if not exists insurance_dw.prem_src4
            select
                age_buy,
                Nursing_Age,
                sex,
                T_Age,
                ppp,
                bpp,
                interest_rate,
                sa,
                policy_year,
                age,
                ppp_,
                bpp_,
                qx,
                kx,
                qx_ci,
                qx_d,
               cast (spark_udaf(qx,lx) over(partition by age_buy,sex,ppp order by policy_year) as decimal(17,12)) as lx
            from prem_src4_1;
        """).show()

    # 4.数据输出

    # 5.关闭SparkContext
    spark.stop()
