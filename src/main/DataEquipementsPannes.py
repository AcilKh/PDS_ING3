from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf().setAppName("READ-EQUIPEMENTS-PANNES").setMaster("local[1]")
sc = SparkContext(spark_conf)
# Création de la session Spark
spark = SparkSession.builder.config(spark_conf).appName("CSVtoHDFS").getOrCreate()
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
# Lecture du fichier CSV local en tant que DataFrame
df = spark.read.csv("data.csv", header=True, inferSchema=True)
# Écriture du DataFrame sur HDFS
df.write.csv("hdfs://192.168.1.2:9000/bronze/data_acil_steph", header=True)
# Arrêt de la session Spark
spark.stop()
#
