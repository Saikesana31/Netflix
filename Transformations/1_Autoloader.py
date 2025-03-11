# Databricks notebook source
# MAGIC  %md
# MAGIC # Incremental Data Loading using AutoLoader

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema netflix_catalog.net_schema;
# MAGIC

# COMMAND ----------

checkpoint_location = "abfss://silver@netflixproject31.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
                     .option("cloudFiles.format","csv")\
                     .option("cloudFiles.schemaLocation",checkpoint_location)\
                     .load("abfss://raw@netflixproject31.dfs.core.windows.net")

# COMMAND ----------

df.display()

# COMMAND ----------

df.writeStream.option("checkpointLocation", checkpoint_location)\
              .trigger(processingTime = "10 seconds")\
              .start("abfss://bronze@netflixproject31.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

