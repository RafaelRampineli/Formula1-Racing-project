# Formula1-Racing-project
![image](https://github.com/user-attachments/assets/743f2f50-d93c-4b8d-b110-811d47aa6ace)

TO DO: Pending add Unity Catalog Databricks

📖 Overview
In this project, I explore a range of datasets related to Formula 1, including race results, driver statistics, lap times, and more. Focus on ingesting raw race data from multiple sources, transforming it, and storing it in a Delta Lake to enable fast and reliable data analysis. This project leverages key technologies such as Azure Data Lake Storage, Delta Lake, and PySpark to handle massive amounts of data.

📊 Key Features

Data Exploration: Analyze historical race results, qualifying times, and driver standings.
Visualizations: Create insightful graphs and charts to visualize trends and comparisons (e.g., lap times, speed).
Pipeline: Create a pipeline of ETL using Azure Data Factory orchestration.

🛠️ Technologies Used

Python
Azure Databricks
Azure Data Factory
PySpark
SQL

📁 Repository Structure

/set-up              # Notebooks used to set up and configure how Databricks access Azure Data Lake Gen2 storage.
/dataset             # Contains datasets used
/ingestion           # Notebooks used to ingest data on Databricks
/trans               # Notebooks used to transform data on Databricks
/analysis            # Data Visualization using SQL
/utils               # SQL Incremental load preparation
/includes            # Some common configurations that we can use from others notebooks
/demo                # Some tasks used to validate all project






