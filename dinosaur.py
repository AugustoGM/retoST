from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("DinosaurAnalysis").getOrCreate()
    
    # Leer el dataset
    path_dino = "dinosaur.csv"
    df_dino = spark.read.csv(path_dino, header=True, inferSchema=True)
    
    # Renombrar columnas para evitar espacios en nombres
    df_dino = df_dino.withColumnRenamed("Name", "name") \
                     .withColumnRenamed("Period", "period") \
                     .withColumnRenamed("Diet", "diet") \
                     .withColumnRenamed("Country", "country")
    
    # Crear vista temporal para consultas SQL
    df_dino.createOrReplaceTempView("dinosaurs")
    
    # Mostrar la estructura del dataset
    spark.sql("DESCRIBE dinosaurs").show()
    
    # Obtener todos los dinosaurios carnívoros y herbívoros
    query = """SELECT name, period, diet FROM dinosaurs WHERE diet IN ('carnivore', 'herbivore') ORDER BY period"""
    df_dino_diet = spark.sql(query)
    df_dino_diet.show(20)
    
    # Obtener dinosaurios encontrados en Norteamérica
    query = """SELECT name, country FROM dinosaurs WHERE country LIKE '%North America%'"""
    df_dino_na = spark.sql(query)
    df_dino_na.show(20)
    
    # Contar cuántos dinosaurios hay por periodo
    query = """SELECT period, COUNT(*) as count FROM dinosaurs GROUP BY period ORDER BY count DESC"""
    df_dino_count = spark.sql(query)
    df_dino_count.show()
    
    # Guardar resultados en JSON
    results = df_dino_count.toJSON().collect()
    with open('results/dino_data.json', 'w') as file:
        json.dump(results, file)
    
    # Cerrar sesión de Spark
    spark.stop()
