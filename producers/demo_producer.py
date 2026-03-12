import os
import json
import time
import random
import math
import yfinance as yf
from datetime import datetime
from confluent_kafka import Producer

# --- KAFKA CONFIGURATION ---
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
}

producer = Producer(conf)
TOPIC_MARKET = os.getenv('KAFKA_TOPIC_MARKET')
TOPIC_NEWS = os.getenv('KAFKA_TOPIC_NEWS')
TOPIC_REDDIT = os.getenv('KAFKA_TOPIC_REDDIT')
ASSETS_PATH = os.getenv('ASSETS_PATH')

def load_assets():
    try:
        with open(ASSETS_PATH, 'r') as f:
            return json.load(f)['assets']
    except Exception as e:
        print(f"Error loading config: {e}", flush=True)
        return {}

ASSETS = load_assets()
TICKERS = [a['ticker'] for a in ASSETS]
SECTORS = list(set([a['sector'] for a in ASSETS]))

REGIMES_CONFIG = {
    "BULL": {
        "price_drift": 0.0015,
        "vol_boost": 1.5,
        "news_verbs": ["breaking records", "surging", "dominating"],
        "news_adjectives": ["bullish", "incredible", "unstoppable"],
        "social_templates": [
            "{} is an EXCELLENT investment! Amazing gains!",
            "PROFIT alert! {} is dominating the market!", 
            "I love {}! Best stock ever!"
        ]    
    },
    "BEAR": {
        "price_drift": -0.0012,
        "vol_boost": 3.0,
        "news_verbs": ["crashing", "collapsing", "plummeting"],
        "news_adjectives": ["disastrous", "bloodbath", "uncertain"],
        "social_templates": ["GET OUT NOW! {} is dead.", "I lost everything on {}.", "{} is a total scam."]
    },
    "NEUTRAL": {
        "price_drift": 0.0001,
        "vol_boost": 1.0,
        "news_verbs": ["consolidating", "steadying"],
        "news_adjectives": ["stable", "flat"],
        "social_templates": ["{} is boring today.", "Just watching {}."]
    }
}


def get_anchor_prices():
    print("Fetching anchor prices from Yahoo Finance...")
    data = yf.download(TICKERS, period='1d', interval='1m', group_by='ticker', progress=False)
    anchors = {}
    for t in TICKERS:
        try:
            price = data[t]['Close'].iloc[-1]
            vol = data[t]['Volume'].iloc[-1]
            anchors[t] = {"price": float(price), "volume": int(vol)}
        except:
            anchors[t] = {"price": 100.0, "volume": 1000}
    return anchors

def run_master_simulation():
    current_prices = get_anchor_prices()
    iteration = 0
    sector_regimes = {sector: random.choice(["BULL", "BEAR", "NEUTRAL"]) for sector in SECTORS}
    
    while True:
        # Changing every 2 minutes for demo
        if iteration % 120 == 0:
            for sector in SECTORS:
                sector_regimes[sector] = random.choices(["BULL", "BEAR", "NEUTRAL"], weights=[40, 30, 30])[0]
                print(f"Switching {sector} sector to {sector_regimes[sector]}.")

        timestamp = datetime.now().isoformat()

        for asset in ASSETS:
            t = asset['ticker']
            sector = asset['sector']
            clean_ticker = t.replace("-USD", "")
            regime = sector_regimes[sector]
            config = REGIMES_CONFIG[regime]
            
            # --- 1. HIGH VOLABILITY MARKET DATA ---
            shock_chance = random.random()
            is_shock = shock_chance < 0.15  # 15% di probabilità di uno shock

            if is_shock:
                shock = random.uniform(0.01, 0.03) * random.choice([-1, 1])
                vol_multiplier = random.randint(20, 50)
            else:
                shock = 0
                vol_multiplier = 1

            change = config["price_drift"] + shock + random.uniform(-0.0005, 0.0005)
            current_prices[t]["price"] *= (1 + change)

            base_vol = random.randint(100, 500)
            volume = int(base_vol * config["vol_boost"] * vol_multiplier)
                        
            market_msg = {
                'timestamp': timestamp,
                'ticker': clean_ticker,
                'price': round(current_prices[t]["price"], 4),
                'volume': volume,
                'sector': sector
            }
            producer.produce(TOPIC_MARKET, key=clean_ticker, value=json.dumps(market_msg))

            # News (10% prob)
            if random.random() < 0.10:
                news_msg = {
                    'timestamp': timestamp,
                    'source': 'FinancialWire',
                    'title': f"{clean_ticker} {random.choice(config['news_verbs'])}: market is {random.choice(config['news_adjectives'])}",
                    'description': 'Market analysis and breaking financial updates.',
                    'url': f'http://news-sim.com/{clean_ticker}/{random.getrandbits(16)}',
                    'related_tickers': [clean_ticker]
                }
                producer.produce(TOPIC_NEWS, key=clean_ticker, value=json.dumps(news_msg))

            # Social (30% prob)
            if random.random() < 0.30:
                social_msg = {
                    'timestamp': timestamp,
                    'author': f'trader_{random.randint(100, 999)}',
                    'title': 'Market Discussion',
                    'body': random.choice(config['social_templates']).format(clean_ticker),
                    'score': random.randint(1, 1000),
                    'num_comments': random.randint(0, 50),
                    'upvote_ratio': round(random.uniform(0.7, 1.0), 2),
                    'related_tickers': [clean_ticker],
                    'source': 'Reddit'
                }
                producer.produce(TOPIC_REDDIT, key=clean_ticker, value=json.dumps(social_msg))

        producer.flush()
        iteration += 1
        time.sleep(1)

if __name__ == "__main__":
    run_master_simulation()