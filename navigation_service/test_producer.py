from confluent_kafka import Producer

# 1. Configure the Producer to point to your local Kafka Broker running in Docker
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# 2. Define your topic and a dummy message
topic = 'nav-events'
dummy_message = '{"event": "obstacle_found", "reaction": "Oh! its blocking me here! Let me check it out."}'

# 3. Send the message
producer.produce(topic, value=dummy_message.encode('utf-8'))

# 4. Ensure the message actually leaves the Python script before it closes
producer.flush()

print("SUCCESS: Message sent to Kafka!")