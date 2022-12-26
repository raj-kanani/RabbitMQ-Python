import pika
import sys

"""
To receive all logs: python receive_topic.py "#"
To receive all logs from "kern": python receive_topic.py "kern.*"
To receive all critical logs: python receive_topic.py "*.critical"
Can create multiple bindings: python receive_topic.py "kern.*" "*.critical"

To emit log with key "kern.critical": python send_topic.py "kern.critical" "A critical kernel error"
"""

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs',
                         exchange_type='topic')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]
if not binding_keys:
    sys.stderr.write("Usage: %s receive topic message...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange='topic_logs',
                       queue=queue_name,
                       routing_key=binding_key)

print(' Waiting for logs and receive topic message')


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))


channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
