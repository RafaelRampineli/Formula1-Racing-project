-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lesson Objectives
-- MAGIC
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DEMO;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES;
--SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lesson Objectives
-- MAGIC 1. Created Managed table using Python
-- MAGIC 2. Created Managed table using SQL
-- MAGIC 3. Effect of droping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.write.format("Delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lesson Objectives
-- MAGIC 1. Created external table using Python
-- MAGIC 2. Created external table using SQL
-- MAGIC 3. Effect of droping a managed table
-- MAGIC
-- MAGIC ## ATTENTION: THIS JUST WORK WHEN YOU ARE'NT USING UNITY CATALOG!!!!!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ex_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ex_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1555/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo;

INSERT INTO demo.race_results_ext_sql
SELECT * FROM  demo.race_results_ex_py 
WHERE race_year = 2020;

-- COMMAND ----------

Show tables in demo;

-- COMMAND ----------

-- when dropping external table, data createad on blob storage won't be deleted.
-- you must delete the blob storage manually
drop table demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](/Workspace/Formula1-Racing-Project/Screenshot_1.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # View on Tables
-- MAGIC
-- MAGIC ##Learning Objects
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

-- temp view just can be used in the same session
CREATE OR REPLACE TEMP VIEW v_race_results
as 
  select * 
  from demo.race_results_python;

select * from demo.v_race_results


-- COMMAND ----------

-- global temp view can be used in all sessions
CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
as 
  select * 
  from demo.race_results_python;

select * from demo.global_temp.gv_race_results;

-- COMMAND ----------

-- permanent view are persisted in hive metastora and can be used after cluster restart
CREATE OR REPLACE VIEW pv_race_results
as 
  select * 
  from demo.race_results_python;

select * from demo.pv_race_results;