# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###parameters

# COMMAND ----------

dbutils.widgets.text("sourcefolder", "netflix_directors")
dbutils.widgets.text("targetfolder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###variables
# MAGIC

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_trg_folder = dbutils.widgets.get("targetfolder")


# COMMAND ----------

print(var_src_folder)
print(var_trg_folder)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(f"abfss://bronze@netflixprojectdlwidman.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta") \
    .mode("append") \
    .option("path", f"abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/{var_trg_folder}") \
    .save()
