-- Databricks notebook source
/* TOTAL OF POINTS BY DRIVERS

we're restricting the number of races, because driver that raced once time would be a ranked higher compared to the one's who raced on more races but got a few points as well.
So we want to probably say that, we will only include drivers who have raced on certain number of races at least, because that way you've got a bigger sample of data to analyze it rather than just looking at a few.

I want total races is > 50.

As we can see, Ayrton Senna is the top 2 drivers in the world
*/
select driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by driver_name
having(count(1) >= 50)
order by avg_points desc

-- COMMAND ----------

/*DOMINANT DRIVERS LAST DECADE */
select driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
having(count(1) >= 50)
order by avg_points desc

-- COMMAND ----------

select driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by driver_name
having(count(1) >= 50)
order by avg_points desc

-- COMMAND ----------

