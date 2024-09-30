# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC dbutils.widgets.help()
# MAGIC
# MAGIC display(dbutils.fs.mounts())
# MAGIC
# MAGIC df.show()
# MAGIC
# MAGIC df.printSchema()
# MAGIC
# MAGIC df.describe().show()
# MAGIC
# MAGIC type(df)
# MAGIC
# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formual1555/processed/circuits
# MAGIC
# MAGIC display(spark.read.parquet('/mnt/formual1555/processed/circuits'))
# MAGIC
# MAGIC display(dbutils.fs.mounts())