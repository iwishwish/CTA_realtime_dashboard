import logging
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

def contains_substring(to_test, substr):
    _before, match, _after = to_test.partition(substr)
    return len(match) > 0

def topic_pattern_match(pattern):
    """
        Takes a string `pattern`
        Returns `True` if one or more topic names contains substring `pattern`.
        Returns `False` if not.
    """
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics()
    topics = topic_metadata.topics
    filtered_topics = {key: value for key, value in topics.items() if contains_substring(key, pattern)}
    return len(filtered_topics) > 0

def build_topic(topic_name, broker_url=None, config_dict=None, 
                num_partitions=1, num_replicas=1):
    """
        creates the topic if it does not already exist on the Kafka Broker
    """
    if topic_exists(topic_name):
        return
    
    if None==broker_url:
        broker_url = "PLAINTEXT://localhost:9092"
        
    if None==config_dict:
        config_dict = {'cleanup.policy': 'delete',
                       'delete.retention.ms': 2000,
                       'file.delete.delay.ms': 2000}
        
    new_topic = NewTopic(topic=topic_name, 
                         num_partitions=num_partitions, 
                         replication_factor=num_replicas,
                         config = config_dict)
    
    client = AdminClient({"bootstrap.servers": broker_url})
    futures = client.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"topic {topic_name} created")
        except Exception as e:
            logger.debug(f"failed to create topic {topic_name}: {e}")
            
    return
    
