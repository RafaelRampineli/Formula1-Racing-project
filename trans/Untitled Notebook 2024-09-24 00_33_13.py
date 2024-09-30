# Databricks notebook source
#using widgets to pass notebook parameters
dbutils.widgets.text("file_date", "2021-03-28")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

dbutils.notebook.run("1.race_results", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("2.driver_standings", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("3.constructor_standings", 0 , {"data_source": "Ergast API", "file_date": var_filedate})