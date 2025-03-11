# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="weekdayLookup",key="weekoutput")

# COMMAND ----------

print(var)

# COMMAND ----------

