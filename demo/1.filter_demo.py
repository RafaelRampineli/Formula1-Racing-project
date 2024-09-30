# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')


# COMMAND ----------

# It's possible change word FILTER by WHERE and result will be the same

# Filter using Python Notation (you can choose one of then)
display(races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5 )))
#display(races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5 )))

# Filter using SQL Notation
display(races_df.where("race_year == 2019 AND round <= 5"))


# COMMAND ----------

#