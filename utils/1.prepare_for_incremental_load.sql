-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;
DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1555/processed";

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1555/presentation";