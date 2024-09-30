# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

#using widgets to pass notebook parameters
dbutils.widgets.text("file_date", "2021-03-21")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

# repeat this for any other notebook
dbutils.notebook.run("1.Ingest_circuits_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("2.Ingest_Races_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("3.Ingest_Constructor_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("4.Ingest_Drivers_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("5.Ingest_Results_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("6.Ingest_multiLines_Json_pitstop_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("7.Ingest_multiple_CSV_LapTimes_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("8.Ingest_multiple_Json_Qualifying_file", 0 , {"data_source": "Ergast API", "file_date": var_filedate})

# COMMAND ----------

dbutils.notebook.run("../trans/1.race_results", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("../trans/2.driver_standings", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("../trans/3.constructor_standings", 0 , {"data_source": "Ergast API", "file_date": var_filedate})
dbutils.notebook.run("../trans/5.Calculated_race_results_py", 0 , {"data_source": "Ergast API", "file_date": var_filedate}