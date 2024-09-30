# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports used

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType  
from pyspark.sql.functions import col, current_timestamp, lit, concat, to_timestamp

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

# Use this way when you don't have createad a database inside databricks and save the data in a folder.
# We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

races_schemas = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True),
])

races_df = spark.read \
        .option("header",True) \
        .schema(races_schemas) \
        .csv(f'{raw_folder_path}/{var_filedate}/races.csv')

# COMMAND ----------

#races_df = spark.sql("select * from f1_raw.races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Adding news columns race_timestamp and ingestion_date on Dataframe

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only necessary columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(   col("raceId"), 
                                                      col("year"), 
                                                      col("round"), 
                                                      col("circuitId"), 
                                                      col("name"), 
                                                      col("race_timestamp"),
                                                      col("ingestion_date")
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Renamed some columns name

# COMMAND ----------

# Renamed some Columns
races_final_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Writing data to Datalake Partition By Race Year

# COMMAND ----------

# Writing data to parquet
#races_final_df.write.mode("overwrite") \
#.partitionBy('race_year') \
#.parquet("f{processed_folder_path}/races")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")    

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking Location and Type
# MAGIC DESC EXTENDED f1_processed.races

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.races
# MAGIC
# MAGIC --display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

#%fs
#ls /mnt/formula1555/processed/races

#circuits_final_df.show()
#circuits_final_df.printSchema()
#circuits_final_df.describe().show()
#type(circuits_final_df)

#display(spark.read.parquet("/mnt/formula1555/processed/races"))

#display(dbutils.fs.mounts())