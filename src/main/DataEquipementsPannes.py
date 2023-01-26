from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class DataEquipementsPannes:
    def put_hdfs(self):
        spark_conf =  SparkConf().setAppName("READ-EQUIPEMENTS-PANNES").setMaster("spark://192.168.1.15:7077").set("spark.submit.deployMode", "client")
        sc = SparkContext(spark_conf)
# Création de la session Spark
        spark = SparkSession.builder.appName("CSVtoHDFS").getOrCreate()
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
# Lecture du fichier CSV local en tant que DataFrame
        #df = spark.read.csv("data.csv", header=True, inferSchema=True)
# Écriture du DataFrame sur HDFS
        #df.write.csv("hdfs://192.168.1.2:9000/bronze/data_acil_steph", header=True)
# Arrêt de la session Spark
        spark.stop()
#
