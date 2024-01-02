from pyspark.sql import SparkSession

# Inicializar una sesión de Spark
spark = SparkSession.builder.appName("yelp").master("yarn").getOrCreate()

# Leer archivos JSON desde HDFS
#df_business = spark.read.json("hdfs://yelp-master:/data/yelp_academic_dataset_business.json")
df_review = spark.read.json("hdfs://yelp-master:/data/yelp_academic_dataset_review.json")

"""
TODO Operaciones / Consultas
"""

# Muestra los datos
df_review.show()

# Cierra la sesión de Spark
spark.stop()

