# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Define a function to add an ingestion date column
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

#Method 2: 
#Use SaveAsTable to first insertion, and use InsertInto for subsequent insertions

#Attention: 
#saveAsTable uses the partition column and adds it at the end. 
#insertInto works using the order of the columns (exactly as calling an SQL insertInto) instead of the columns name. 

#You can check this information at dislay above.

#So to work, we must select our columns putting de partition column at last column of dataset.

#We need to set the spark.sql.sources.partitionOverwriteMode setting to dynamic, the dataset needs to be partitioned, and the write mode overwrite.
#Using this set, when data will be written and overwrite just the partition.

def overwrite_partition(input_df, database_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  
  if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{database_name}.{table_name}")
  else:
    output_df.write.mode("append").partitionBy(f"{partition_column}").format("parquet").saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# Reorder partition column to satisfact the def overwrite_partition function InsertInto
def re_arrange_partition_column(input_df, partition_column):
    column_list = []

    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    return input_df.select(column_list)

# COMMAND ----------

# we will select only the distinct years of the races.
# By doing this, we will create a list of unique race years using the collect() function. 
# However, we specifically need a list of years, such as 2021, to use as a filter for retrieving the desired race results data.

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select("race_year").distinct().collect()

    column_value_list = []
    for row in df_row_list:
        column_value_list.append(row[column_name])
    return column_value_list