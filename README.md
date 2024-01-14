# PROYECTO ATBD: YELP

## Consultas realizadas

#### Resultados y gráficos en "consultas_spark.ipynb"
1. Obtener los 10 negocios con mayor número de revisiones
2. Obtener las 10 categorías con la mayor puntuación media
3. Obtener las 10 ciudades con la mayor puntuación media
4. Calcular la media de palabras para las reseñas de cada puntuación (1-5 estrellas)
5. Obtener las 10 categorías que más se repiten para cada puntuación (1-5 estrellas)
6. Analizar cómo un atributo determinado afecta a la puntuación del negocio
7. Obtener la media anual de puntuación para las 10 categorías con mayor número de reseñas.


## Instrucciones para el despliegue y ejecución de las consultas en AWS EC2

1. Descargar ficheros de este repositorio
2. Arrancar AWS Academy
3. Crear instancias Hadoop e instalar YARN
   - seguir pasos en: https://github.com/memaldi/hadoop-ansible-ec2
4. Descargar, instalar y configurar Spark:
   ```bash
   ansible-playbook -i inventory.yml --key-file=~/.ssh/vockey.pem --user ec2-user install-spark.yml
   ```
5. Introducir los datos en HDFS
   ```bash
   hadoop-3.3.6/bin/hdfs dfs -mkdir /data
   ```
   ```bash
   hadoop-3.3.6/bin/hdfs dfs -put /data/* /data
   ```

6. Comprobar que los NodeMaganers están lanzados:
   ```bash
   hadoop-3.3.6/bin/yarn node -list
   ```
   También de pueden observar desde la Web UI (introduciendo la IP pública del master):
   
   http://ec2-18-232-80-108.compute-1.amazonaws.com:8088/cluster
   
   Si los nodos no están activados se pueden activar desde cada uno de los workers:
   ```bash
   hadoop-3.3.6/bin/yarn nodemanager
   ```

7. Iniciar trabajo Spark en YARN desde el nodo cliente:

   ```bash
   spark-3.5.0-bin-hadoop3/bin/spark-submit \
     --deploy-mode client \
     --num-executors 3 \
     /app/YELP.py
   ```

   Comando completo (los parámetros adicionales ya se han introducido desde la app):
   ```bash
   spark-3.5.0-bin-hadoop3/bin/spark-submit \
     --master yarn \
     --deploy-mode client \
     --num-executors 3 \
     --executor-memory 1g \
     --executor-cores 1 \
     /app/YELP.py
   ```

8. Resultado
   
   Se imprime por consola los resultados de las consultas y se almacenan en HDFS
   
   Ver fichero parquet:
   ```
   hadoop-3.3.6/bin/hdfs dfs -ls /user/ec2-user/consultas/consulta_<n>
   ```
