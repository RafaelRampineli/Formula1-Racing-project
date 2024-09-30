# Databricks notebook source
#using widgets to pass notebook parameters
dbutils.widgets.text("data_source", "")
var_datasource = dbutils.widgets.get("data_source")

dbutils.widgets.text("file_date", "2021-03-28")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Finding race year for which the data is to be reprocessed
# MAGIC
# MAGIC To find the year for which the data needs to be reprocessed instead of the race ID, we will use the race results file, including its date. First, we will filter the results by this date. This will provide all the data corresponding to that date in the race results table. Next, we will select only the distinct years of the races.
# MAGIC
# MAGIC By doing this, we will create a list of unique race years using the collect() function. However, we specifically need a list of years, such as 2021, to use as a filter for retrieving the desired race results data.

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{var_filedate}'")

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we will filter the race year so that we can use the method to call the race year as the column name. 
# MAGIC You can access this method within the function and specify a list of years.
# MAGIC
# MAGIC This list allows us to check if the race year in the data retrieved from our Parquet file matches any of the years we’ve listed. If it does, that data will be included in the DataFrame; if not, it will be excluded.

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))
    
constructor_standing_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") ==1, 1)).alias("wins"))    

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(window))

# COMMAND ----------

# Writing data to parquet
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# Using Delta Lake:  input_df, db_name, table_name, folder_path, merge_condition, partition_column):
merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation','constructor_standings', presentation_folder_path, merge_condition, 'race_year')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings

# COMMAND ----------

