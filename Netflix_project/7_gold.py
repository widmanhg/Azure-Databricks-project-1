# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #DLT_GOLD-LAYER

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

looktables_rules={
    "rule1": " show_id is NOT NULL"

}

# COMMAND ----------

@dlt.table(
    name="gold_netflixdirectors"
)
@dlt.expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix directors")
    return df


# COMMAND ----------

@dlt.table(
    name="gold_netflixcast"
)
@dlt.expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix cast")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_netflixcountries"
)
@dlt.expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix countries")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_netflixcategory"
)
@dlt.expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix category")
    return df


# COMMAND ----------

@dlt.table

def gold_stg_netflixtitles():
    df = spark.readStream.format("delta").load("abfss://silver@netflixprojectdlwidman.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

@dlt.view

def gold_trns_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("newflag", lit(1))
    return df

# COMMAND ----------


masterdata_rules={
      "rule1": "newflag is NOT NULL",
      "rule2": "show_id is NOT NULL"
      }


  

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df