import os
import json
import time
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
}

TOPIC_NAME = os.getenv('KAFKA_TOPIC_REDDIT')
ASSETS_PATH = os.getenv('ASSETS_PATH')
DATA_PATH = os.getenv('REDDIT_SIMULATION_DATA_PATH')
producer = Producer(conf)

def load_assets():
    with open(ASSETS_PATH, 'r') as f:
        return json.load(f)['assets']

ASSETS = load_assets()

def get_mentioned_tickers(text):
    if not text or not isinstance(text, str): return []
    found = []
    text_lower = text.lower()
    for a in ASSETS:
        keywords = [a['ticker'].lower(), a['name'].lower()] + [k.lower() for k in a.get('search_keywords', [])]
        if any(f" {kw} " in f" {text_lower} " for kw in keywords):
            found.append(a['ticker'])
    return list(set(found))

def run_simulator():
    print(f"Simulated Reddit Producer started.", flush=True)
    cols_to_use = ['id', 'author', 'title', 'selftext', 'score', 'num_comments', 'upvote_ratio']
    df = pd.read_csv(DATA_PATH, usecols=cols_to_use)
    
    print(f"Dataset loaded: {len(df)} rows. Starting streaming simulation...", flush=True)

    while True:
        for _, row in df.iterrows():
            full_text = f"{row['title']} {row['selftext']}"
            mentioned = get_mentioned_tickers(full_text)
            
            if mentioned:
                payload = {
                    'timestamp': datetime.now(timezone.utc).isoformat(), # changing to now to simulate realtime
                    'original_id': row['id'],
                    'author': row['author'],
                    'title': row['title'],
                    'body': row['selftext'][:1000] if isinstance(row['selftext'], str) else "",
                    'score': int(row['score']),
                    'num_comments': int(row['num_comments']),
                    'upvote_ratio': float(row['upvote_ratio']),
                    'related_tickers': mentioned,
                }
                
                producer.produce(
                    topic=TOPIC_NAME,
                    key=str(row['id']),
                    value=json.dumps(payload).encode('utf-8')
                )
                
                producer.poll(0)
                
                print(f"Simulated Reddit post for {mentioned}", flush=True)
                time.sleep(2) 
        
        print("Finished available data. Restarting...", flush=True)
        producer.flush()

if __name__ == "__main__":
    run_simulator()