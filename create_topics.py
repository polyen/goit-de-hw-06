from kafka.admin import KafkaAdminClient, NewTopic
from config import kafka_config, topic_prefix

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )

    topic_names = [f'{topic_prefix}_sensors_streaming',
                   f'{topic_prefix}_output_streaming']
    num_partitions = 2
    replication_factor = 1

    new_topics = []

    for topic_name in topic_names:
        new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        new_topics.append(new_topic)

    try:
        for new_topic in new_topics:
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            [print(topic) for topic in admin_client.list_topics() if new_topic.name in topic]
    except Exception as e:
        print(f"An error occurred: {e}")

    print(admin_client.list_topics())

    admin_client.close()

if __name__ == '__main__':
    create_topics()