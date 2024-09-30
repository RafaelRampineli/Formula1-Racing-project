-- Databricks notebook source
-- specifying the location here because we are going to create managed tables at this database, not the external tables. And that means we don't want to specify the location,
--we want the location to be picked up from the database itself.

--So what will happen is any table we create on the database, which is a managed table, would be created under this Mount.
--If we don't specify the location, the data will be created under the Databricks default location.

--We want the data to be created in our storage account that we've specifically created to keep the data.

--to Check Default location when we don't specify run: DESC DATABASE <database>;
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1555/processed"

-- COMMAND ----------

