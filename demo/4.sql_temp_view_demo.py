# Databricks notebook source
# MAGIC %md
# MAGIC # Access dataframes using SQL
# MAGIC
# MAGIC To run SQL queries, you need to register your DataFrame as a temporary view
# MAGIC
# MAGIC 1. Create Temp view on Dataframes
# MAGIC 2. Access view from SQL cell
# MAGIC 3. Access view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

p_value = 2020
spark.sql(f"Select * from v_race_results where race_year = {p_value}").show()

# COMMAND ----------

