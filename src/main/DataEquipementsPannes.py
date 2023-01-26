from pyspark.sql import SparkSession

class DataEquipementsPannes:
    def put_hdfs(self):
# Création de la session Spark
        spark = SparkSession.builder.appName("CSVtoHDFS").getOrCreate()
# Lecture du fichier CSV local en tant que DataFrame
        df = spark.read.csv("data.csv", header=True, inferSchema=True)
# Écriture du DataFrame sur HDFS
        df.write.csv("hdfs://hostname:port/user/data/", header=True)
# Arrêt de la session Spark
        spark.stop()
#
