import json
import random
import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
"""
-------------------------------------------------
   Description :	TODO：模拟数据源的代码，可以把数据写入到Kafka中
   SourceFile  :	Demo09_IOTSource
   Author      :	81196
   Date	       :	2023/9/21
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 2.数据输入
#2.1 构建Kafka的生产者
producer = KafkaProducer(
    bootstrap_servers=['node1:9092', 'node2:9092', 'node3:9092'],
    acks='all',
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)
#2.2 物联网设备类型
deviceTypes = ["洗衣机", "油烟机", "空调", "窗帘", "灯", "窗户", "煤气报警器", "水表", "燃气表"]

#2.3 模拟数据生成
while True:
    index = random.choice(range(0, len(deviceTypes)))
    deviceID = f'device_{index}_{random.randrange(1, 20)}'
    deviceType = deviceTypes[index]
    deviceSignal = random.choice(range(10, 100))

    # 组装数据集
    print({'deviceID': deviceID, 'deviceType': deviceType, 'deviceSignal': deviceSignal,
           'time': time.strftime('%Y%m%d')})

    # 发送数据
    producer.send(topic='iot',
                  value={'deviceID': deviceID, 'deviceType': deviceType, 'deviceSignal': deviceSignal,
                                   'time': time.strftime('%Y%m%d')}
    )

    # 间隔时间 5s内随机
    time.sleep(random.choice(range(1, 5)))

