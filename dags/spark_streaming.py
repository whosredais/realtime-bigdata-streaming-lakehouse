from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Initialisation de la session Spark
# On ajoute le package Maven 'spark-sql-kafka' pour que Spark sache lire depuis Kafka.
# La version 3.3.0 correspond à la version de Spark dans nos conteneurs bde2020.
spark = SparkSession.builder \
    .appName("WebLogsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# On réduit les logs de Spark dans la console pour ne voir que les erreurs (WARNING)
spark.sparkContext.setLogLevel("WARN")

# 2. Définition du schéma JSON
# On explique à Spark à quoi ressemble la donnée qui arrive de Kafka
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True), # On va le convertir en vrai format 'Date' juste après
    StructField("method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("user_agent", StringType(), True)
])

# 3. Connexion à Kafka en Streaming
# Vu que ce script va tourner *à l'intérieur* de l'infrastructure Docker (via Airflow/SparkMaster), 
# on utilise le nom du conteneur "kafka" et le port interne "9092".
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsing et Transformation
# Les messages Kafka arrivent en brut (binaire). On les transforme en texte puis on applique notre schéma JSON.
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# On convertit le champ texte "timestamp" en vrai type 'timestamp' pour que Spark puisse calculer des durées
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# 5. Filtrage et Agrégation
# On ne garde que les logs en erreur (Status >= 400)
errors_df = parsed_df.filter(col("status") >= 400)

# On compte le nombre d'erreurs par Code HTTP (ex: 404, 500) sur des "fenêtres glissantes" de 10 secondes.
# Le 'watermark' permet à Spark de gérer d'éventuels retards dans l'arrivée des logs.
windowed_counts = errors_df.withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("status")
    ).count()

# 6. Écriture des résultats dans HDFS (Hadoop)
# On écrit directement dans le cluster Hadoop (hdfs://namenode:9000) au format CSV.
# Le 'checkpointLocation' est crucial en streaming : il permet à Spark de se rappeler où il s'est arrêté 
# en cas de plantage pour reprendre exactement au bon message.
query = windowed_counts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/output/web-errors") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/web-errors") \
    .start()

# On empêche le script de se terminer, il doit écouter Kafka en permanence
query.awaitTermination()
