# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter("race_year = 2019") \
    .withColumnRenamed('name', 'race_name')
circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name')


# COMMAND ----------

# MAGIC %md
# MAGIC INNER JOIN
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join

# COMMAND ----------

# 2 option for make JOINS between tables
display(circuits_df.join(races_df, on='circuit_id', how='inner') \
        .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round))


display(circuits_df.join(races_df, on=circuits_df.circuit_id == races_df.circuit_id, how='inner') \
        .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round))    

# COMMAND ----------

# MAGIC %md
# MAGIC OUTER JOINS

# COMMAND ----------

#Left outer Join
display(circuits_df.join(races_df, on=circuits_df.circuit_id == races_df.circuit_id, how='left') \
        .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round))    

# semi join works as a inner join, but just return information from Left table       

# CrossJoin
 races_df.crossJoin(circuits_df)