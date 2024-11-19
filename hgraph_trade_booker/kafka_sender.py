from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')


def send_to_kafka(topic: str, message: str):
    """
    Send a message to a Kafka topic.
    """
    producer.send(topic, message.encode('utf-8'))
    producer.flush()
    print(f"Message sent to Kafka topic: {topic}")