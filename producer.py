from kafka import KafkaProducer
from config import kafka_config, topic_prefix
import json
import uuid
import time
import random
from datetime import datetime

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    return producer

def run_producer():
    producer = create_producer()
    topic_name = f'{topic_prefix}_sensors_streaming'
    sensor_id = uuid.uuid4().hex
    while True:
        try:
            data = {
                "timestamp": time.time(),
                "temperature": round(random.uniform(25, 45), 0),
                "humidity": round(random.uniform(15, 85), 0),
                "sensor_id": sensor_id,
            }

            producer.send(topic_name, key=str(uuid.uuid4()), value=data)
            producer.flush()

            print(f'{sensor_id}: Message sent to topic {topic_name} with value {data}')
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as ex:
            print(f'An error occurred: {ex}')

    producer.close()
    print(f'{sensor_id}: Producer closed at {time.time()}')

if __name__ == '__main__':
    run_producer()
