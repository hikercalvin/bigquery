with 
action_log_with_dt as(
  select *
  ,substr(string(timestamp), 1, 10) as dt
FROM `ga360-203507.sample_data_GA.action_log`
)
,action_day_count_per_user as(
  select
    Users,
    count(distinct dt) as action_day_count
  from
 action_log_with_dt
 group by Users
)

select 
  action_day_count,
  count(distinct Users) as User_count,
  --組成百分比
  100.0*count(distinct Users)/sum(count(distinct Users)) over() as Composition_ratio,
  --累計百分比
  100.0*sum(count(distinct Users))over(order by action_day_count rows between unbounded preceding and current row)/sum(count(distinct Users)) over() as Cumulative_ratio
from
  action_day_count_per_user
group by
  action_day_count
order by
  action_day_count 

