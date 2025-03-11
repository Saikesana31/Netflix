# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT Notebook-Goldlayer

# COMMAND ----------

looktables_rules = {
    "rule1" : "show_id is not null"
}

# COMMAND ----------

@dlt.table( name = "gold_netflixdirectors")
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixproject31.dfs.core.windows.net/netflixdirectors")
    return df

# COMMAND ----------

@dlt.table(name = "gold_netflixcast")
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixproject31.dfs.core.windows.net/netflixcast")
    return df

# COMMAND ----------

@dlt.table(name = "gold_netflixcountries")
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixproject31.dfs.core.windows.net/netflixcountries")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)
@dlt.expect_or_drop("rule1","show_id is not null")

def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixproject31.dfs.core.windows.net/netflixcategory")
    return df

# COMMAND ----------

@dlt.table

def gold_stg_netflixtitles():
    df = spark.readStream.format("delta").load("abfss://silver@netflixproject31.dfs.core.windows.net/netflixtitles")
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view

def gold_trns_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withcolumn("newflag",lit(1))
    return df

# COMMAND ----------

masterdata_rules = {
    "rule1" : "newflag is not null",
    "rule2" : "show_id is not null"
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(masterdata_rules)

def gold_netflixtitles():
    df = df.spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df