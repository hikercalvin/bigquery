with visit_number as(
  select 
   Users,
   min(timestamp) as first_action_date,
   Sessions_count
  from `ga360-203507.sample_data_GA.action_log`
  where Sessions_count=1
  group by
   Users,
   Sessions_count)

,action_log_with_mst_users as(
  select 
    u.Users,
    date(u.first_action_date) as first_action_date,
    date(a.timestamp) as action_date,
    max(date(a.timestamp)) over() as latest_date
from visit_number as u
  left outer join
  `ga360-203507.sample_data_GA.action_log` as a
on u.Users = a.Users
)
select
  *
from  
  action_log_with_mst_users
order BY 
  first_action_date, Users



