
"""
使用API代码来模拟Kafka的生产者
"""
from kafka import KafkaProducer

#1.创建KafkaProducer对象
producer = KafkaProducer(bootstrap_servers=['node1:9092','node2:9092','node3:9092'],acks=-1)

#2.使用生产者对象，向Kafka集群生产数据
#get：表示同步发送
#producer.send(topic='test01',value=b'good good study,day day up!').get()
#producer.send(topic='test01',value='good good study,day day up!'.encode('UTF-8')).get()
#future = producer.send(topic='test01',value='勤学如春起之苗，不见其长，日有所增!'.encode('UTF-8'))
for i in range(5):
    #有了key的参数后，分发策略就是对key进行hash取模了
    #producer.send(topic='test02', value=f'勤学如春起之苗，不见其长，日有所增!{i}'.encode('UTF-8'),partition=0).get()
    #producer.send(topic='test02',key=f'key={i}'.encode("UTF-8"), value=f'勤学如春起之苗，不见其长，日有所增!{i}'.encode('UTF-8')).get()
    producer.send(topic='test02',value=f'勤学如春起之苗，不见其长，日有所增!{i}'.encode('UTF-8')).get()

#异步：刷新数据到Kafka中
#producer.flush()