#!/usr/bin/env python
import pika
import time
import json
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='35.237.232.19'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    data = json.loads(body)     
    print(" Nombre: {}".format(data['nombre']))     
    print(" Depto: {}".format(data['departamento']))   
    print(" Edad: {}".format(data['edad']))     
    print(" Forma de contagio: {}".format(data['forma']))   
    print(" Estado: {}".format(data['estado']))     
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()