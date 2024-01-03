import pyspark
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

# Rutas de los ficheros JSON
business_path_json = "/data/yelp_academic_dataset_business.json"
review_path_json = "/data/yelp_academic_dataset_review.json"

# Rutas de HDFS para las dos tablas
business_path = "/data/yelp_academic_dataset_business"
review_path = "/data/yelp_academic_dataset_review"

consultas_path = "/user/ec2-user/consultas/"

# Verifica si las tablas existen y las crea las tablas si no existen
if not spark.status(business_path, strict=False) is not None:
    print("Generando tabla bussiness")
    spark.read.json(business_path_json).write.parquet(business_path, mode="overwrite")
    print("Se ha generado la tabla business")

if not spark.status(review_path, strict=False) is not None:
    print("Generando tabla review")
    spark.read.json(review_path_json).write.parquet(review_path, mode="overwrite")
    print("Se ha generado la tabla review")

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
print("\n Realizando consulta 1:")
print("\n \t Obtener los 10 negocios con mayor número de revisiones")

top_businesses = df_review.groupBy("business_id").agg(count("review_id").alias("num_reviews")) \
    .orderBy(col("num_reviews").desc()).limit(10)

result1 = top_businesses.join(df_business, "business_id").select("name", "num_reviews").orderBy(col("num_reviews").desc())
result1.show()

result1.write.parquet(consultas_path+"consulta1")

# Consulta 2
print("\n Realizando consulta 2:")
print("\n \t Obtener las 10 categorías con la mayor puntuación media")
avg_stars_by_category = df_business.select("business_id", "stars", "categories") \
    .withColumn("category", explode(split(col("categories"), ", "))) \
    .groupBy("category").agg(avg("stars").alias("avg_stars")) \
    .orderBy(col("avg_stars").desc()).limit(10) \
    .orderBy(col("category"))

result2 = avg_stars_by_category.show()
result2.write.parquet(consultas_path+"consulta2")

# Consulta 3
print("\n Realizando consulta 3:")
print("\n \t Obtener las 10 ciudades con la mayor puntuación media")
avg_stars_by_city = df_business.select("city", "stars") \
    .groupBy("city").agg(avg("stars").alias("avg_stars")) \
    .orderBy(col("avg_stars").desc()).limit(10) \
    .orderBy(col("city"))

result3 = avg_stars_by_city.show()
result3.write.parquet(consultas_path+"consulta3")

# Consulta 4
print("\n Realizando consulta 4:")
print("\n \t Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas) ")
# Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas)
avg_words_by_stars = df_review.groupBy("stars").agg(avg(size(split(col("text"), " "))).alias("avg_words"))

result4 = avg_words_by_stars.show()
result4.write.parquet(consultas_path+"consulta4")

# Consulta 5
print("\n Realizando consulta 5:")
print("\n \t Obtener las 10 categorías que más se repiten para cada puntuación (1-5 estrellas)")
windowSpec = Window.partitionBy("stars").orderBy(col("count").desc())

top_categories_by_stars = (
    df_review
    .join(df_business, "business_id")
    .select("stars", explode(split("categories", ", ")).alias("category"))
    .groupBy("stars", "category")
    .agg(count("*").alias("count"))
    .withColumn("row_number", row_number().over(windowSpec))
    .filter(col("row_number") <= 10)
    .drop("row_number")
    .orderBy("stars", col("count").desc())
)

result5 = top_categories_by_stars.show(500)
result5.write.parquet(consultas_path+"consulta5")

# Consulta 6
print("\n Realizando consulta 6:")
print("\n \t Analizar cómo un atributo determinado afecta a la puntuación del negocio")

attribute_effect = df_business.select("stars", "attributes.ByAppointmentOnly") \
    .groupBy("ByAppointmentOnly").agg(avg("stars").alias("avg_stars"))

result6 = attribute_effect.show()
result6.write.parquet(consultas_path+"consulta6")

# Consulta 7
print("\n Realizando consulta 7:")
print("\n \t Obtener la media anual de puntuación para las 10 categorías con mayor número de reseñas")

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
df_review = df_review.withColumnRenamed("stars", "review_stars")

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

result7.show(400)
result7.write.parquet(consultas_path+"consulta7")

# Cierra la sesión de Spark
spark.stop()

