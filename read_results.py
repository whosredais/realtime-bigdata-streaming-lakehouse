from pyspark.sql import SparkSession

# 1. Initialiser une session Spark locale
spark = SparkSession.builder.appName("ReadParquetResults").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*50)
print("Lecture des résultats depuis le Datalake HDFS...")
print("="*50 + "\n")

# 2. Lire le dossier contenant tous les fichiers Parquet
try:
    df = spark.read.parquet("hdfs://namenode:9000/output/web-errors")
    
    # 3. Afficher le schéma
    print("--- STRUCTURE DES DONNÉES ---")
    df.printSchema()

    # 4. Afficher le contenu
    print("\n--- RÉSULTATS DE L'AGRÉGATION (FENÊTRES DE 10 SECONDES) ---")
    df.orderBy("window.start").show(truncate=False)
except Exception as e:
    print(f"Erreur lors de la lecture : {e}")
    print("Êtes-vous sûr que les fichiers Parquet ont été générés ?")

spark.stop()
