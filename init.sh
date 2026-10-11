#!/bin/bash
echo "Création des répertoires pour Airflow..."
mkdir -p ./dags ./logs ./plugins

echo "Lancement des conteneurs en tâche de fond..."
docker-compose up -d

echo "L'infrastructure démarre ! Voici les accès Web prévus d'ici 1 à 2 minutes :"
echo "- HDFS NameNode : http://localhost:9870"
echo "- Spark Master : http://localhost:8080"
echo "- Airflow Webserver : http://localhost:8082 (User: admin / Pass: admin)"
