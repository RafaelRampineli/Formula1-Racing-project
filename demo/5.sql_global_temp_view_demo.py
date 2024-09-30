# Databricks notebook source
# MAGIC %md
# MAGIC # Global Temp Views
# MAGIC
# MAGIC To run SQL queries, you need to register your DataFrame as a temporary view
# MAGIC
# MAGIC 1. Create Global temp view on Dataframes
# MAGIC 2. Access view from SQL cell
# MAGIC 3. Access view from Python cell
# MAGIC 4. Access view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# To access global temp, we must put the database name in the SQL query
%sql
select count(1) from global_temp.gv_race_results

# COMMAND ----------

p_value = 2020
spark.sql(f"Select count(1) from global_temp.gv_race_results where race_year = {p_value}").show()

# COMMAND ----------

