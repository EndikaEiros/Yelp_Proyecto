# PROYECTO: YELP - ARQUITECTURA TECNOLÓGICA PARA BIG DATA

## Consultas realizadas

1. Obtener los 10 negocios con mayor número de revisiones
2. Obtener las 10 categorías con la mayor puntuación media
3. Obtener las 10 ciudades con la mayor puntuación media
4. Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas)
5. Obtener las 10 categorías que más se repiten para cada puntuación (1-5 estrellas)
6. Analizar cómo un atributo determinado afecta a la puntuación del negocio
7. Obtener la media anual de puntuación para las 10 categorías con mayor número de
reseñas.
### Resultados y gráficos en "consultas_spark.ipynb"

## Instrucciones para la ejecución en AWS EC2

1. Descargar ficheros de este repositorio
2. Arrancar AWS Academy
3. Crear instancias Hadoop e instalar YARN (seguir pasos en https://github.com/memaldi/hadoop-ansible-ec2)
4. Descargar, instalar y configurar Spark:
```
ansible-playbook -i inventory.yml --key-file=~/.ssh/vockey.pem --user ec2-user install-spark.yml
```
5. Iniciar trabajo Spark desde el nodo cliente en modo YARN:

```
spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors <num_executors> \
  --executor-memory <memoria_por_executor> \
  /app/YELP.py
```

