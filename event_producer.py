import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

class EventProducer:
    def __init__(self, bootstrap_servers):
        self.conf = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.conf)
        self.topics = {
            'add_to_cart': 'ecommerce_events',
            'purchase': 'ecommerce_events'
        }
        self.sku_list = ["SKU-1001", "SKU-1002"]  # Only two SKUs

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def generate_event(self):
        event_type = random.choices(
            ['add_to_cart', 'purchase'],
            weights=[0.7, 0.3]
        )[0]
        
        base_event = {
            "user_id": fake.uuid4(),
            "product_id": random.choice(self.sku_list),  # Choose from the 2 SKUs
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quantity": fake.random_int(1, 5)
        }
        
        if event_type == 'purchase':
            base_event.update({
                "event_type": "purchase",
                "price": round(random.uniform(10, 500), 2),
                "payment_method": random.choice(["credit_card", "paypal", "debit_card"])
            })
        else:
            base_event.update({
                "event_type": "add_to_cart",
                "price": None,
                "payment_method": None
            })
            
        return base_event

    def produce_events(self, interval=0.5):
        try:
            while True:
                event = self.generate_event()
                self.producer.produce(
                    topic=self.topics[event['event_type']],
                    value=json.dumps(event).encode('utf-8'),
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                time.sleep(interval)
        except KeyboardInterrupt:
            self.producer.flush()
            print("Producer stopped")

if __name__ == "__main__":
    producer = EventProducer("localhost:9092")
    producer.produce_events()