import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# 1. Initialisation de Faker (outil qui génère des données réalistes de test)
fake = Faker()

def get_producer():
    """Initialisation différée du producteur Kafka avec support local et conteneurisé."""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092', 'localhost:29092'],
        client_id='python-log-generator',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

topic = 'web-logs'

# 3. Fonction de callback (Vérification de la livraison)
def on_send_success(record_metadata):
    pass # Succès silencieux

def on_send_error(excp):
    print(f"Erreur d'envoi à Kafka : {excp}")

# 4. Préparation des données pour simuler un vrai serveur Web
methods = ["GET", "POST", "PUT", "DELETE"]
statuses = [200, 200, 200, 200, 200, 201, 301, 400, 401, 404, 500, 500, 502, 503]

def generate_log():
    """Génère un seul log au format dictionnaire Python"""
    return {
        "ip": fake.ipv4(),
        "timestamp": datetime.utcnow().isoformat(),
        "method": random.choice(methods),
        "url": fake.uri_path(),
        "status": random.choice(statuses),
        "user_agent": fake.user_agent()
    }

# 5. Boucle principale
if __name__ == '__main__':
    print(f"Début de l'envoi en continu vers le topic Kafka '{topic}'...")
    print("Appuie sur Ctrl+C pour arrêter le script.")
    producer = get_producer()
    
    try:
        while True:
            log_data = generate_log()
            
            # On envoie au topic 'web-logs'
            producer.send(topic, value=log_data).add_callback(on_send_success).add_errback(on_send_error)
            
            # On affiche le log dans la console
            print(f"Envoyé : {log_data}")
            
            time.sleep(random.uniform(0.1, 1.0))
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        print("Vidage des derniers messages en attente...")
        producer.flush()
        print("Fin du script.")
