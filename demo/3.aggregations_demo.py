# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregations

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

demo_df.select(countDistinct("race_name")).show()
demo_df.select(sum("points")).show()
demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points")).show()
demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Group by

# COMMAND ----------

demo_df \
    .groupBy("driver_name") \
    .sum("points") \
    .show()

demo_df \
    .groupBy("driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
    .show()    

# COMMAND ----------

# MAGIC %md
# MAGIC # Window Functions

# COMMAND ----------

demo2_df = race_results_df.filter("race_year in (2019, 2020) ")

# COMMAND ----------

demo2_grp = demo2_df \
    .groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

window = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo2_grp.withColumn("rank", rank().over(window)).show(100)