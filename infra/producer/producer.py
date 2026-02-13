import time # permet d'utiliser sleep() pour faire une pause
import json # permet de transformer un dictionnaire Python en JSON
import requests # permet d'appeler une API HTTP 
from kafka import KafkaProducer # KafkaProducer : l'objet qui permet d'envoyer des messages dans kafka

# Configuration API
API_KEY="d66uuchr01qmckkbcubgd66uuchr01qmckkbcuc0" #la clé pour accéder à l'API Finnhub
BASE_URL= "https://finnhub.io/api/v1/quote" #URL de l'API
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"] #Liste des actions qu'on veut surveiller

# Création du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"], #adresse du serveur kafka, kafka tourne dans Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Fonction qui récupère le prix d'une action
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol} : {e}")
        return None
    
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send("stock-quotes", value=quote)
    time.sleep(6)
