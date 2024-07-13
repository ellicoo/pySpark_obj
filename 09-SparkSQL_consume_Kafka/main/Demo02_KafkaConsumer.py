
"""
模拟Kafka的消费者，来消费Kafka集群的数据
"""
from kafka import KafkaConsumer

#1.创建消费者对象
consumer = KafkaConsumer('test01',group_id='my-group',
                         bootstrap_servers=['node1:9092','node2:9092','node3:9092'],
                         enable_auto_commit=False)

#2.消费Kafka的数据
for message in consumer:
    #获取数据的topic
    print(message.topic)
    #获取数据的分区
    print(message.partition)
    #获取数据的偏移量
    print(message.offset)
    #获取数据的key
    print(message.key)
    #获取数据的 value
    print(message.value.decode("UTF-8"))
    #打印数据的对象
    print(message)

#3.手动提交offset偏移量
consumer.commit()