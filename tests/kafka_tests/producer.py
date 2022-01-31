from kafka import KafkaProducer

TOPIC_NAME = 'items'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

for x in range(50):
    producer.send(TOPIC_NAME, b'ab')
    producer.flush()

