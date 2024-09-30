# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Running configuration Notebook. 
# MAGIC ##### Now we can import the configuration and use inside this notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#using widgets to pass notebook parameters
dbutils.widgets.text("data_source", "")
var_datasource = dbutils.widgets.get("data_source")

dbutils.widgets.text("file_date", "2021-03-21")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the spark Dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType  
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# Use this way when you don't have createad a database inside databricks and save the data in a folder.
# We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

circuits_schemas = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", DoubleType(), True),
                                      StructField("url", StringType(), True),
])

circuits_df = spark.read \
        .option("header",True) \
        .schema(circuits_schemas) \
        .csv(f'{raw_folder_path}/{var_filedate}/circuits.csv')


# COMMAND ----------

# circuits_df = spark.sql("Select * from f1_raw.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only necessary columns

# COMMAND ----------


circuits_selected_df = circuits_df.select(  col("circuitId"), 
                                            col("circuitRef"), 
                                            col("name"), 
                                            col("location"), 
                                            col("country"), 
                                            col("lat"), 
                                            col("lng"), 
                                            col("alt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Renamed some columns name

# COMMAND ----------

# Renamed some Columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Adding new column on Dataframe

# COMMAND ----------

# To add a literal value we should use a function lit
circuits_final_df = add_ingestion_date(circuits_renamed_df) \
    .withColumn("env", lit("Production")) \
    .withColumn("datasource", lit(var_datasource))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Writing data to Datalake as parquet

# COMMAND ----------

# Writing data to parquet
# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking Location and Type
# MAGIC DESC EXTENDED f1_processed.circuits

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")