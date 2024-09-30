-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

/*the position tells us which position the driver finish the race on and then the points tells us how many points he scored in the race. These are the information we need.

Problems!
For finishing first, you get eight points in 1954, but the points never stayed the same.
If we look here for finishing first in 2012, you get 25 points.
A finishing first in 1953, you've got nine points.
So the points never stayed the same.

if we add up the points and say this is the dominant driver or this is the dominant team, which is not going to be right, because for finishing first in 2012, we've got 25 points.

Solution: we are going to calculate the points ourselves.
So the way you do that is you can use the position,what I'm going to do is if you finish first, I'm going to give ten points. Finishing second, you get nine points like that,
it goes down up to finishing 10th, you get one point and after that you get no points, basically.

*/
create table f1_presentation.calculated_race_results
using parquet
as
select  races.race_year,
        constructors.name as team_name,
        drivers.name as driver_name,
        results.position,
        results.points,
        11 - results.position as calculated_points
from results
join drivers on results.driver_Id = drivers.driver_Id
join constructors on results.constructor_Id = constructors.constructor_Id
join races on results.race_Id = races.race_Id
where results.position <= 10


-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

