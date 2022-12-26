import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print('Waiting for messages.')


def callback(ch, method, properties, body):
    print(" Received %r" % body)
    time.sleep(body.count(b'.'))
    print("Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume('task_queue', callback)

channel.start_consuming()
