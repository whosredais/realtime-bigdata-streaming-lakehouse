from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Définition des paramètres par défaut du DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Création du DAG
with DAG(
    'big_data_streaming_pipeline',
    default_args=default_args,
    description='Pipeline de bout en bout pour le traitement de logs en temps réel',
    schedule_interval=None, # On le déclenchera manuellement pour ce test
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['streaming', 'spark', 'kafka', 'hdfs'],
) as dag:

    # 1. Vérification de HDFS
    # On utilise une commande Bash pour faire un simple 'ping' HTTP sur l'API du NameNode
    check_hdfs = BashOperator(
        task_id='check_hdfs_is_up',
        bash_command="curl -sf http://namenode:9870 > /dev/null && echo 'HDFS est OPÉRATIONNEL' || exit 1",
    )

    # 2. Vérification de Kafka
    # On utilise l'utilitaire réseau 'nc' (netcat) pour s'assurer que le port 9092 de Kafka est ouvert et écoute
    check_kafka = BashOperator(
        task_id='check_kafka_is_up',
        bash_command="nc -z kafka 9092 && echo 'Kafka est OPÉRATIONNEL' || exit 1",
    )

    # 3. Lancement du générateur de logs
    # Comme c'est une boucle infinie, on l'envoie en tâche de fond (nohup ... &) 
    # pour que l'opérateur Airflow passe au statut 'Success' sans rester bloqué éternellement.
    trigger_log_generator = BashOperator(
        task_id='trigger_log_generator',
        bash_command="""
        echo "Installation des dépendances Python manquantes dans Airflow..."
        pip install kafka-python-ng faker > /dev/null 2>&1
        echo "Démarrage du script générateur..."
        nohup python /opt/airflow/dags/log_generator.py > /opt/airflow/logs/generator_airflow.log 2>&1 &
        echo "Le générateur tourne en arrière-plan !"
        """,
    )

    # 4. Soumission du Job Spark Streaming
    # L'opérateur officiel SparkSubmitOperator va générer la commande 'spark-submit' pour nous
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_streaming_job',
        application='/opt/airflow/dags/spark_streaming.py', # Emplacement du script dans le conteneur
        conn_id='spark_default', # La connexion définie dans l'UI d'Airflow
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
        name='Airflow-WebLogsStreaming',
        verbose=True
    )

    # 5. Définition de l'ordre d'exécution (Graphe)
    # Les deux vérifications (Kafka et HDFS) tournent en parallèle. 
    # Quand elles réussissent, on lance le générateur, puis le Job Spark.
    [check_hdfs, check_kafka] >> trigger_log_generator >> submit_spark_job
