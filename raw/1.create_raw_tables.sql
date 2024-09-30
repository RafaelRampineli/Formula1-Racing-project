-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating External Tables using SQL from CSV
-- MAGIC If was managed tables, we aren't declaring options like: USING and OPTIONS

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1555/raw/circuits.csv", header true);

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1555/raw/races.csv", header true)

-- COMMAND ----------

DESC EXTENDED f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating External Tables using SQL from JSON
-- MAGIC If was managed tables, we aren't declaring options like: USING and OPTIONS

-- COMMAND ----------

-- Single line JSON - Simple Structure
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/formula1555/raw/constructors.json");

-- Single line JSON - Simple Structure
DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS(path "/mnt/formula1555/raw/results.json");


-- Single line JSON - Complex Structure because NAME is a struct
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1555/raw/drivers.json");

-- MultiLine line JSON - Simple Structure
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS(path "/mnt/formula1555/raw/pit_stops.json", multiLine true);


-- Single line JSON - Simple Structure - Multiple Files, so our path are on folder
DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1555/raw/lap_times");

-- MultiLine line JSON - Simple Structure - Multiple Files
DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
USING json
OPTIONS (path "/mnt/formula1555/raw/qualifying", multiLine true);


-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

