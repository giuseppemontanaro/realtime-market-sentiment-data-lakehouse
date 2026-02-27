import os
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
}

TOPIC_NAME = os.getenv('KAFKA_TOPIC')
DELAY = int(os.getenv('PRODUCER_DELAY', 2))

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        ticker = msg.key().decode('utf-8')
        print(f"[Kafka] Ticker: {ticker} | Partition: {msg.partition()} | Offset: {msg.offset()}")


producer = Producer(conf)

def generate_market_data():
    tickers = ['BTC', 'ETH', 'AAPL', 'TSLA', 'EUR/USD']
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),        
        'ticker': random.choice(tickers),
        'price': round(random.uniform(1.2, 65000.0), 2),
        'volume': random.randint(1, 500),
        'exchange': 'Aiven-Market-Sim'
    }

print(f"Producer started! Sending data to topic '{TOPIC_NAME}' every 2 seconds...")

try:
    while True:
        data = generate_market_data()
        
        producer.produce(
            topic=TOPIC_NAME,
            key=data['ticker'], 
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        
        # Throttling to stay within Aiven Free Tier limits
        time.sleep(2)

except KeyboardInterrupt:
    print("\Manual interruption received. Flushing buffer...")
finally:
    producer.flush()