-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Rules: Considering teams that raced over 100 times.
-- MAGIC
-- MAGIC #Insights:
-- MAGIC based on result of this analysis, we can confirm that Mercedes Team become the top 1 World Formula 1 Constructors.
-- MAGIC Followed by Ferrari and MacLaren.
-- MAGIC The last decade Redbull winner many times and become top 2 World Formula 1 Constructors.
-- MAGIC But until 2015, Ferrari was the Top 1 World Formula 1 Constructors.
-- MAGIC
-- MAGIC while Mercedes has consistently evolved and improved, Ferrari has faced hurdles that have hindered their performance, impacting their ability to compete at the highest level.
-- MAGIC
-- MAGIC ## How Mercede's rise to the Top in Formula 1 and why Ferrari declined?
-- MAGIC
-- MAGIC Mercedes has consistently pushed the boundaries of technology. Their power units have been among the most powerful and efficient, which gives them an edge on both speed and fuel management.
-- MAGIC The leadership of Toto Wolff and a cohesive team environment have fostered a culture of success. Effective communication and collaboration between engineering and trackside teams have been critical.
-- MAGIC And the key changed game called Lewis Hamilton and Valtteri Bottas (and now George Russell) has provided a strong competitive advantage. Hamilton, in particular, has shown remarkable consistency and skill, translating to points and championships.
-- MAGIC
-- MAGIC On other hands, Ferrari has faced criticism for their race strategies and decision-making during critical moments, often losing valuable points due to poor calls.
-- MAGIC In recent seasons, Ferrari has struggled with car reliability, leading to retirements and lost opportunities in races.
-- MAGIC While Ferrari has made strides in performance, they have sometimes lagged behind in crucial areas, like aerodynamics and power unit development, especially when compared to Mercedes.
-- MAGIC
-- MAGIC So while Mercedes has consistently evolved and improved, Ferrari has faced hurdles that have hindered their performance, impacting their ability to compete at the highest level.

-- COMMAND ----------

-- TOTAL OF POINTS BY TEAMS
select team_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having(count(1) >= 100)
order by avg_points desc

-- COMMAND ----------

-- TOTAL OF POINTS BY TEAM LAST DECADE 
select team_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name
having(count(1) >= 100)
order by avg_points desc

-- COMMAND ----------

select team_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2020
group by team_name
having(count(1) >= 100)
order by avg_points desc

-- COMMAND ----------

-- TOTAL OF POINTS BY TEAMS UNTIL 2015
select team_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year <= 2015
group by team_name
having(count(1) >= 100)
order by avg_points desc

-- COMMAND ----------

