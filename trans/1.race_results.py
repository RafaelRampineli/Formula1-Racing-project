# Databricks notebook source
# MAGIC %md
# MAGIC # Read dataframes from parquet

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

# MAGIC %md
# MAGIC ![](../RaceResults_DER.png)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# renamed columns because has same name
# reading data from folder as delta file
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number","driver_number") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("nationality","driver_nationality") 

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","team") 

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location") 

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp", "race_date") 
    
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{var_filedate}'") \
    .withColumnRenamed("time","race_time") \
    .withColumnRenamed("race_id","result_race_id") \
    .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC --   select * from f1_processed.results
# MAGIC --   where race_id = 800 and driver_id = 612
# MAGIC
# MAGIC --select * from f1_processed.drivers
# MAGIC --where name = 'Andy Linden'
# MAGIC -- select * from f1_presentation.race_results where race_id = 800 and driver_name = 'Andy Linden'

# COMMAND ----------

# drv_df = spark.sql("select [number] as driver_number, [name] as driver_name, nationality as driver_nationality 
#                    from f1_processed.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC Join Circuits with Races 
# MAGIC Join rest dataframes
# MAGIC Select columns needed
# MAGIC add column needed

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

races_circuits_df = races_df.join(circuits_df, on="circuit_id", how="inner") \
  .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_df.race_id, how="inner") \
          .join(drivers_df, on="driver_Id", how="inner") \
          .join(constructors_df, on="constructor_Id", how="inner") \
  .select( races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date,
          circuits_df.circuit_location,          
          drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality,
          constructors_df.team,
          results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.position, results_df.result_file_date) \
  .withColumn("created_date", current_timestamp()) \
  .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

#Check if data is correct
#Using this website to validate the data:> https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results

# display(race_results_df.filter((race_results_df.race_year == 2020) & (race_results_df.race_name == 'Abu Dhabi Grand Prix')) \
#     .orderBy(race_results_df.points.desc()))

# COMMAND ----------

# Writing data to parquet
# race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
#race_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

#overwrite_partition(race_results_df, 'f1_presentation', 'race_results', 'race_id')

# Using Delta Lake:  input_df, db_name, table_name, folder_path, merge_condition, partition_column):
merge_condition = 'tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id'
merge_delta_data(race_results_df, 'f1_presentation','race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id, driver_name, count(1) 
# MAGIC -- from f1_presentation.race_results
# MAGIC -- group by race_id, driver_name
# MAGIC -- having count(1) > 1;
# MAGIC
# MAGIC select * from f1_presentation.race_results where race_id = 800 and driver_name = 'Andy Linden'
# MAGIC -- drop table f1_presentation.race_results;
# MAGIC -- drop table f1_presentation.calculated_race_results;
# MAGIC -- drop table f1_presentation.driver_standings;
# MAGIC -- drop table f1_presentation.constructor_standings;