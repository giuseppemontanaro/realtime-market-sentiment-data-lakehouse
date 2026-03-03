import os
import json
import time
import math
import yfinance as yf
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
}

TOPIC_NAME = os.getenv('KAFKA_TOPIC_MARKET')
ASSETS_PATH = os.getenv('ASSETS_PATH')
producer = Producer(conf)

def load_assets():
    try:
        with open(ASSETS_PATH, 'r') as f:
            assets = json.load(f)['assets']
            return {a['ticker']: a for a in assets}
    except Exception as e:
        print(f"Error loading config: {e}", flush=True)
        return {}


ASSETS_MAP = load_assets()
TICKERS = list(ASSETS_MAP.keys())


last_sent_timestamp = None

def stream_last_minute_data():
    global last_sent_timestamp
    
    print(f"Yahoo Finance Producer started.", flush=True)

    while True:
        print(f"\n--- Fetching batch for {len(TICKERS)} tickers ---", flush=True)
        try:
            data = yf.download(TICKERS, period='1d', interval='1m', group_by='ticker', progress=False)
            
            if data.empty:
                print("No data received from Yahoo, retring...", flush=True)
                time.sleep(10)
                continue
            
            # taking last 3 rows to fill gaps eventually
            recent_df = data.tail(3) 
            
            for timestamp, row in recent_df.iterrows():
                
                # continuing if timestamp already sent
                if last_sent_timestamp is not None and timestamp <= last_sent_timestamp:
                    continue

                for symbol in TICKERS:
                    tick = row.get(symbol)
                    
                    if tick is None or tick.empty:
                        continue

                    price = tick.get('Close')
                    volume = tick.get('Volume')

                    if price is None or volume is None or math.isnan(price):
                        continue
                    
                    safe_volume = int(volume) if not math.isnan(volume) else 0

                    asset_info = ASSETS_MAP.get(symbol, {})
                    
                    message = {
                        'timestamp': timestamp.isoformat(),
                        'ticker': symbol,
                        'price': round(float(price), 2),
                        'volume': safe_volume,
                        'sector': asset_info.get('sector', 'Unknown'),
                        'name': asset_info.get('name', symbol)
                    }
                    
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=symbol,
                        value=json.dumps(message).encode('utf-8')
                    )
                
                producer.flush()
                print(f"Sent unique tick for {timestamp}", flush=True)
                last_sent_timestamp = timestamp
                time.sleep(0.5)

            time.sleep(30) 

        except Exception as e:
            print(f"Error in streaming loop: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    stream_last_minute_data()