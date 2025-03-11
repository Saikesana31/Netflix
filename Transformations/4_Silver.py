# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Data Transformations

# COMMAND ----------

df = spark.read.format("delta")\
                .option("header",True)\
                .option("inferSchema",True)\
                .load("abfss://bronze@netflixproject31.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.fillna({"duration_minutes":0, "duration_seasons":1})
df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
        .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

df.printSchema()

# COMMAND ----------

df = df.withColumn("shorttitle",split(col("title"), ":")[0])
df.display()

# COMMAND ----------

df = df.withColumn("rating",split(col("rating"), "-")[0])
df.display()

# COMMAND ----------

df = df.withColumn("type_flag",when(col('type') == "Movie",1)\
                           .when(col('type') == "TV Show",2)\
                            .otherwise(0))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### rank the movies based on duration minutes 

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
df.display()

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_view

# COMMAND ----------

## session-scoped views
df.createOrReplaceGlobalTempView("global_view") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *,
# MAGIC dense_rank() over(order by duration_minutes desc) as duration_ranking_sql
# MAGIC from temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from global_temp.global_view

# COMMAND ----------

# MAGIC %sql
# MAGIC   select
# MAGIC   type,
# MAGIC   count(*) as total_count
# MAGIC   from temp_view
# MAGIC   GROUP BY type

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("*").alias("count"))
display(df_vis)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://silver@netflixproject31.dfs.core.windows.net/netflix_titles")\
        .save()

# COMMAND ----------

