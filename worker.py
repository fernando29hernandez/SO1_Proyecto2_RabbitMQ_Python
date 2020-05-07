#!/usr/bin/env python
import pika
import time
import json
import pymongo
import redis

myclientmongo = pymongo.MongoClient("mongodb://35.237.232.19:27017/")
myclientredis = redis.Redis(
    host='35.237.232.19',
    port=6379, 
    password='',
    db=0
    )

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    data = json.loads(body)     
    print(" [x] Nombre: {}".format(data['nombre']))     
    print(" [x] Depto: {}".format(data['departamento']))   
    print(" [x] Edad: {}".format(data['edad']))     
    print(" [x] Forma de contagio: {}".format(data['forma']))   
    print(" [x] Estado: {}".format(data['estado']))     
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # insert on mongo
    mydb = myclientmongo["proyecto"]
    mycol = mydb["casos"]
    x = mycol.insert_one(data)
    # insert on redis
    myclientredis.set(str(x.inserted_id),body)


if __name__ == "__main__":
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='35.237.232.19'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()
