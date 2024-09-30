-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
select  team_name,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points,
        rank() over (order by avg(calculated_points) desc) as rank_team
from f1_presentation.calculated_race_results
group by team_name
having(count(1) >= 100)
order by avg_points desc

-- COMMAND ----------

select  race_year,
        team_name,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where rank_team <= 5)
group by race_year, team_name
order by race_year, avg_points desc

-- COMMAND ----------

