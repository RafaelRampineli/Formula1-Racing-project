# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1555/demo'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1555/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC External Tables

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1555/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating a table using delta to read data
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1555/demo/results_external';
# MAGIC
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# but if we don't want create a table to read this, we can just use DF.read
results_external_df = spark.read.format("delta").load("/mnt/formula1555/demo/results_external")
display(results_external_df)


# COMMAND ----------

# Saving data partitioned by constructorid
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Update Delta Table 
# MAGIC # 2. Delete Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Updating using SQL
# MAGIC SELECT * FROM f1_demo.results_managed;
# MAGIC
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;
# MAGIC
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# Using python with predicate SQL
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1555/demo/results_managed")

deltaTable.delete("position <= 10") 
deltaTable.update("position <= 10", { "points": "21 - position" } ) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert Delta Table using Merge

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1555/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname");

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1555/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"));

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1555/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"));

# COMMAND ----------

# Creating a view to be useful on a SQL statament
drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")
drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a delta table managed because LOCATION was not specified
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC  ON tgt.driverId = upd.driverId
# MAGIC  WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC  WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp);
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC    UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC  WHEN NOT MATCHED
# MAGIC    THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp);

# COMMAND ----------

# Using PySpark
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1555/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. History & Versioning
# MAGIC # 2. Time Travel
# MAGIC # 3. Vaccum

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC  %sql SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-23T23:51:34.000+00:00';

# COMMAND ----------

# Using Pyspark
df = spark.read.format("delta").option("timestampAsOf", '2021-06-23T15:40:33.000+0000').load("/mnt/formula1555/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql VACUUM f1_demo.drivers_merge
# MAGIC -- used to remove  data from table (like truncate), but by default 7 days stay saved
# MAGIC -- to remove ALL DATA we must put RETAIN 0 HOURS
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC  %sql SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2021-06-23T15:40:33.000+0000';

# COMMAND ----------

# MAGIC %md
# MAGIC #Recovering data from History table using MERGE

# COMMAND ----------

# MAGIC  %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC     ON (tgt.driverId = src.driverId)
# MAGIC  WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #Delta Lake: Transaction LOGS

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC  driverId INT,
# MAGIC  dob DATE,
# MAGIC  forename STRING, 
# MAGIC  surname STRING,
# MAGIC  createdDate DATE, 
# MAGIC  updatedDate DATE
# MAGIC  )
# MAGIC  USING DELTA

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC  %sql
# MAGIC  INSERT INTO f1_demo.drivers_txn
# MAGIC  SELECT * FROM f1_demo.drivers_merge
# MAGIC  WHERE driverId = 1;

# COMMAND ----------

for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")


# COMMAND ----------

# MAGIC %md
# MAGIC # Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# used in data files and no tables like above
df = spark.table("f1_demo.drivers_convert_to_delta")
df.write.format("parquet").save("/mnt/formula1555/demo/drivers_convert_to_delta_new")

CONVERT TO DELTA parquet.'/mnt/formula1555/demo/drivers_convert_to_delta_new'