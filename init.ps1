Write-Host "Création des répertoires pour Airflow..."
New-Item -ItemType Directory -Force -Path .\dags
New-Item -ItemType Directory -Force -Path .\logs
New-Item -ItemType Directory -Force -Path .\plugins

Write-Host "Lancement des conteneurs en tâche de fond..."
docker-compose up -d

Write-Host "L'infrastructure démarre ! Voici les accès Web prévus d'ici 1 à 2 minutes :"
Write-Host "- HDFS NameNode : http://localhost:9870"
Write-Host "- Spark Master : http://localhost:8080"
Write-Host "- Airflow Webserver : http://localhost:8082 (User: admin / Pass: admin)"
