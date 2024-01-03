import pyspark
# import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt

from datetime import datetime, date
from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, count, explode, split, upper, expr, collect_list, size, split, year, row_number

# Inicializar una sesión de Spark
spark = SparkSession.builder \
    .appName("yelp") \
    .master("yarn") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Rutas de los ficheros JSON
business_path_json = "/data/yelp_academic_dataset_business.json"
review_path_json = "/data/yelp_academic_dataset_review.json"

# Rutas de HDFS para las dos tablas
business_path = "/data/yelp_academic_dataset_business"
review_path = "/data/yelp_academic_dataset_review"

consultas_path = "/user/ec2-user/consultas/"

# Verifica si las tablas existen y las crea las tablas si no existen
print("\n ------------- Generando tabla bussiness -------------")
spark.read.json(business_path_json).write.parquet(business_path, mode="overwrite")
print("\n ------------- Se ha generado la tabla business -------------")


print("\n ------------- Generando tabla review -------------")
spark.read.json(review_path_json).write.parquet(review_path, mode="overwrite")
print("\n ------------- Se ha generado la tabla review -------------")

# Cargar los datos de la base de datos de 
df_business = spark.read.parquet(business_path)
df_review = spark.read.parquet(review_path)

# Muestra los datos
df_review.show()
df_business.show()

"""
TODO Operaciones / Consultas
"""

# Consulta 1


top_businesses = df_review.groupBy("business_id").agg(count("review_id").alias("num_reviews")) \
    .orderBy(col("num_reviews").desc()).limit(10)

result1 = top_businesses.join(df_business, "business_id").select("name", "num_reviews").orderBy(col("num_reviews").desc())

print("\n Realizando consulta 1:")
print("\n \t Obtener los 10 negocios con mayor número de revisiones")
result1.show()

result1.write.parquet(consultas_path+"consulta1", mode="overwrite")

# Consulta 2

result2 = df_business.select("business_id", "stars", "categories") \
    .withColumn("category", explode(split(col("categories"), ", "))) \
    .groupBy("category").agg(avg("stars").alias("avg_stars")) \
    .orderBy(col("avg_stars").desc()).limit(10) \
    .orderBy(col("category"))

print("\n Realizando consulta 2:")
print("\n \t Obtener las 10 categorías con la mayor puntuación media")
result2.show()
result2.write.parquet(consultas_path+"consulta2", mode="overwrite")

# Consulta 3

result3 = df_business.select("city", "stars") \
    .groupBy("city").agg(avg("stars").alias("avg_stars")) \
    .orderBy(col("avg_stars").desc()).limit(10) \
    .orderBy(col("city"))

print("\n Realizando consulta 3:")
print("\n \t Obtener las 10 ciudades con la mayor puntuación media")
result3.show()
result3.write.parquet(consultas_path+"consulta3", mode="overwrite")

# Consulta 4

# Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas)
result4 = df_review.groupBy("stars").agg(avg(size(split(col("text"), " "))).alias("avg_words"))

print("\n Realizando consulta 4:")
print("\n \t Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas) ")
result4.show()
result4.write.parquet(consultas_path+"consulta4", mode="overwrite")

# Consulta 5

# Renombrar la columna "stars" de review_df a "review_stars"
df_review = df_review.withColumnRenamed("stars", "review_stars")

windowSpec = Window.partitionBy("review_stars").orderBy(col("count").desc())

result5 = (
    df_review
    .join(df_business, "business_id")
    .select("review_stars", explode(split("categories", ", ")).alias("category"))
    .groupBy("review_stars", "category")
    .agg(count("*").alias("count"))
    .withColumn("row_number", row_number().over(windowSpec))
    .filter(col("row_number") <= 10)
    .drop("row_number")
    .orderBy("review_stars", col("count").desc())
)

print("\n Realizando consulta 5:")
print("\n \t Obtener las 10 categorías que más se repiten para cada puntuación (1-5 estrellas)")
result5.show(500)
result5.write.parquet(consultas_path+"consulta5", mode="overwrite")

# Consulta 6


result6 = df_business.select("stars", "attributes.ByAppointmentOnly") \
    .groupBy("ByAppointmentOnly").agg(avg("stars").alias("avg_stars"))

print("\n Realizando consulta 6:")
print("\n \t Analizar cómo un atributo determinado afecta a la puntuación del negocio")
result6.show()
result6.write.parquet(consultas_path+"consulta6", mode="overwrite")

# Consulta 7

# Obtener las 10 categorías con mayor número de reseñas
top_categories_by_reviews = (
    df_review
    .join(df_business, "business_id")
    .select(explode(split("categories", ", ")).alias("category"))
    .groupBy("category")
    .agg(count("*").alias("num_reviews"))
    .orderBy(col("num_reviews").desc())
    .limit(10)
)

# Renombrar la columna "stars" de review_df a "review_stars"
# df_review = df_review.withColumnRenamed("stars", "review_stars")

# Filtrar las reseñas para las categorías seleccionadas
filtered_reviews = (
    df_review
    .join(df_business, "business_id")
    .select("review_stars", "date", explode(split("categories", ", ")).alias("category"))
    .join(top_categories_by_reviews, "category")
)

# Agregar una columna de año y calcular la media anual de puntuación para las 10 categorías con mayor número de reseñas
windowSpec = Window.partitionBy("category", "year").orderBy("year")

result7 = (
    filtered_reviews
    .withColumn("year", year("date"))
    .groupBy("category", "year")
    .agg(avg(col("review_stars")).alias("avg_stars"))
    .withColumn("row_number", row_number().over(windowSpec))
    .filter(col("row_number") == 1)
    .drop("row_number")
    .orderBy("category", "year")
)
print("\n Realizando consulta 7:")
print("\n \t Obtener la media anual de puntuación para las 10 categorías con mayor número de reseñas")
result7.show(400)
result7.write.parquet(consultas_path+"consulta7", mode="overwrite")

# Cierra la sesión de Spark
spark.stop()

