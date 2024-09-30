# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Constructor.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports used

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#using widgets to pass notebook parameters
dbutils.widgets.text("data_source", "")
var_datasource = dbutils.widgets.get("data_source")

dbutils.widgets.text("file_date", "2021-03-21")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark Dataframe reader

# COMMAND ----------

# Use this way when you don't have createad a database inside databricks and save the data in a folder.
# We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

# using DDL schemas
constructors_schemas = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructors_df = spark.read \
        .schema(constructors_schemas) \
        .json(f'{raw_folder_path}/{var_filedate}/constructors.json')


# COMMAND ----------

# constructors_df = spark.sql("Select * from f1_raw.constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns from dataframe

# COMMAND ----------

constructors_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename Columns and Adding new column ingestion_date on Dataframe

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_df) \
    .withColumnRenamed("constructorID", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Writing data to Datalake as a Parquet Format

# COMMAND ----------

# Writing data to parquet
#constructors_final_df.write.mode("overwrite").parquet("f{processed_folder_path}/constructors")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

#%fs
#ls /mnt/formula1555/processed/constructors