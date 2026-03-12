import os
import json
import time
import math
import random
import yfinance as yf
from datetime import datetime
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

def stream_simulated_data():
    global last_sent_timestamp
    print(f"Yahoo Finance Hybrid Producer started.", flush=True)

    # dict for last real data per ticker
    last_real_prices = {symbol: {"price": None, "volume": 0} for symbol in TICKERS}

    while True:
        try:
            # fetch real data
            print(f"\n--- Fetching real anchor data from Yahoo ---", flush=True)
            data = yf.download(TICKERS, period='1d', interval='1m', group_by='ticker', progress=False)
            
            if not data.empty:
                last_row = data.tail(1)
                for symbol in TICKERS:
                    tick = last_row.get(symbol)
                    if tick is not None and not tick.empty:
                        price = tick.get('Close').iloc[0]
                        volume = tick.get('Volume').iloc[0]
                        if not math.isnan(price):
                            last_real_prices[symbol] = {
                                "price": float(price), 
                                "volume": int(volume) if not math.isnan(volume) else 1
                            }

            # simulating new data every second
            print(f"Streaming simulated ticks for the next minute...", flush=True)
            
            for _ in range(60):
                current_time = datetime.now()
                
                for symbol in TICKERS:
                    base_data = last_real_prices[symbol]
                    if base_data["price"] is None: continue

                    noise_price = random.uniform(-0.0005, 0.0005)
                    simulated_price = base_data["price"] * (1 + noise_price)
                    
                    noise_volume = random.uniform(0.8, 1.2)

                    if random.random() > 0.95:
                        noise_factor *= 2 

                    simulated_volume = int((base_data["volume"] / 60) * noise_volume)
                    
                    last_real_prices[symbol]["price"] = simulated_price

                    asset_info = ASSETS_MAP.get(symbol, {})
                    
                    message = {
                        'timestamp': current_time.isoformat(),
                        'ticker': symbol,
                        'price': round(float(simulated_price), 2),
                        'volume': max(1, simulated_volume),
                        'sector': asset_info.get('sector', 'Unknown'),
                        'name': asset_info.get('name', symbol)
                    }

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=symbol,
                        value=json.dumps(message).encode('utf-8')
                    )
                
                producer.flush()
                time.sleep(1)

        except Exception as e:
            print(f"Error in streaming loop: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    stream_simulated_data()