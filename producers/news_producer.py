import os
import json
import time
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
}

producer = Producer(conf)

TOPIC_NAME = os.getenv('KAFKA_TOPIC_NEWS')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
ASSETS_PATH = os.getenv('ASSETS_PATH')
producer = Producer(conf)

def load_assets():
    try:
        with open(ASSETS_PATH, 'r') as f:
            return json.load(f)['assets']
    except Exception as e:
        print(f"Error loading config: {e}", flush=True)
        return []

def build_query(assets):
    names = [a['name'] for a in assets]
    return " OR ".join(f'"{name}"' for name in names)

def get_related_tickers(text, assets):
    found_tickers = []
    text_lower = text.lower()
    for a in assets:
        keywords = [a['ticker'].lower(), a['name'].lower()] + [k.lower() for k in a.get('search_keywords', [])]
        if any(kw in text_lower for kw in keywords):
            found_tickers.append(a['ticker'])
    return list(set(found_tickers))

def fetch_and_produce():
    assets = load_assets()
    if not assets:
        return

    query = build_query(assets)
    url = (f"https://newsapi.org/v2/everything?"
           f"q={query}&"
           f"language=en&"
           f"sortBy=publishedAt&"
           f"pageSize=30&"
           f"apiKey={NEWS_API_KEY}")

    try:
        response = requests.get(url)
        data = response.json()

        if data.get('status') == 'ok':
            articles = data.get('articles', [])
            print(f"[{datetime.now()}] NewsAPI: found {len(articles)} articles.", flush=True)
            
            for art in articles:
                title = art.get('title', '')
                desc = art.get('description', '') or ''
                
                # Dynamic tagging: associating news to tickers based on content
                related = get_related_tickers(title + " " + desc, assets)
                
                payload = {
                    'timestamp': art['publishedAt'],
                    'source': art['source']['name'],
                    'title': title,
                    'description': desc,
                    'url': art['url'],
                    'related_tickers': related,
                    'sector_tags': list(set([a['sector'] for a in assets if a['ticker'] in related]))
                }
                
                producer.produce(
                    topic=TOPIC_NAME,
                    key=art['url'],
                    value=json.dumps(payload).encode('utf-8')
                )
            
            producer.flush()
        else:
            print(f"API Error: {data.get('message')}", flush=True)

    except Exception as e:
        print(f"Error in news loops: {e}", flush=True)

if __name__ == "__main__":
    print("News Producer started.", flush=True)
    while True:
        fetch_and_produce()
        # 15 minuti di attesa (900s) per rispettare i limiti del piano Free
        time.sleep(900)