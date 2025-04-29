# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Array Parameter

# COMMAND ----------

Files = [
    {
        "sourcefolder": "netflix_directors",
        "targetfolder": "netflix directors"
    },
    {
        "sourcefolder": "netflix_cast",
        "targetfolder": "netflix cast"
    },
    {
        "sourcefolder": "netflix_countries",
        "targetfolder": "netflix countries"
    },
    {
        "sourcefolder": "netflix_category",
        "targetfolder": "netflix category"
    }
]


# COMMAND ----------

# MAGIC %md
# MAGIC ##Job Utility to return the array
# MAGIC

# COMMAND ----------

dbutils.jobs.taskValues.set(key ="my_arr", value= Files)