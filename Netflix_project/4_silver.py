# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Silver Data Transformation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://bronze@netflixprojectdlwidman.dfs.core.windows.net/netflix_titles")


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType())) \
       .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("Shorttitle", split(col('title'), ':')[0])

df.display()

# COMMAND ----------

df = df.withColumn(
    "type_flag",
    when(col('type') == "Movie", 1)
    .when(col('type') == "TV Show", 2)
    .otherwise(0)
)

display(df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window = Window.orderBy(col('duration_minutes').desc())

df = df.withColumn(
    "duration_ranking",
    dense_rank().over(window)
)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("*").alias("total count"))
display(df_vis)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix_titles") \
    .save()
