from confluent_kafka import Consumer

# 1. Configure the Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',         # Required: Identifies this listener
    'auto.offset.reset': 'earliest'   # The magic trick: read missed messages from the past
}
consumer = Consumer(conf)

# 2. Subscribe to the channel/topic
topic = 'nav-events'
consumer.subscribe([topic])
print(f"Listening for messages on '{topic}'... (Press Ctrl+C to stop)")

# 3. The Polling Loop (Continuously check the mailbox)
try:
    while True:
        # Wait up to 1 second for a message
        msg = consumer.poll(timeout=1.0) 
        
        if msg is None:
            continue # Mailbox is empty, check again
            
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue
            
        # 4. We got mail! Decode the bytes back to a string and print it.
        received_text = msg.value().decode('utf-8')
        print(f"📬 RECEIVED: {received_text}")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    # Always close the connection cleanly
    consumer.close()