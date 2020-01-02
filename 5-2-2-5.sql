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

,repeat_interval as (
            select '01 day repeat' as index_name, 1 as interval_date
  union all select '02 day repeat' as index_name, 2 as interval_date
  union all select '03 day repeat' as index_name, 3 as interval_date
  union all select '04 day repeat' as index_name, 4 as interval_date
  union all select '05 day repeat' as index_name, 5 as interval_date
  union all select '06 day repeat' as index_name, 6 as interval_date
  union all select '07 day repeat' as index_name, 7 as interval_date
  union all select '08 day repeat' as index_name, 8 as interval_date
  union all select '09 day repeat' as index_name, 9 as interval_date
  union all select '10 day repeat' as index_name, 10 as interval_date
  union all select '11 day repeat' as index_name, 11 as interval_date
  union all select '12 day repeat' as index_name, 12 as interval_date
  union all select '13 day repeat' as index_name, 13 as interval_date
  union all select '14 day repeat' as index_name, 14 as interval_date
  union all select '15 day repeat' as index_name, 15 as interval_date
  union all select '16 day repeat' as index_name, 16 as interval_date
  union all select '17 day repeat' as index_name, 17 as interval_date
  union all select '18 day repeat' as index_name, 18 as interval_date
  union all select '19 day repeat' as index_name, 19 as interval_date
  union all select '20 day repeat' as index_name, 20 as interval_date
  union all select '21 day repeat' as index_name, 21 as interval_date
  union all select '22 day repeat' as index_name, 22 as interval_date
  union all select '23 day repeat' as index_name, 23 as interval_date
  union all select '24 day repeat' as index_name, 24 as interval_date
  union all select '25 day repeat' as index_name, 25 as interval_date
  union all select '26 day repeat' as index_name, 26 as interval_date
  union all select '27 day repeat' as index_name, 27 as interval_date
  union all select '28 day repeat' as index_name, 28 as interval_date
  )
  
,action_log_with_index_date as(
  select 
    u.Users as Users,
    date(u.first_action_date) as first_action_date,
    date(a.timestamp) as action_date,
    max(date(a.timestamp)) over() as latest_date,
    r.index_name,
    date_add(cast(u.first_action_date as date ), interval r.interval_date day) as index_date
from visit_number as u
  left outer join
  `ga360-203507.sample_data_GA.action_log` as a
on u.Users = a.Users
cross join  
  repeat_interval as r
)

, user_action_flag as(
  select
    Users,
    first_action_date,
    index_name,
    sign(
      sum(
        case when index_date <= latest_date then
          case when index_date = action_date then 1 else 0 end
         end
         )
        ) as index_date_action
    from
      action_log_with_index_date
    group by
      Users, first_action_date, index_name, index_date
)
select
  first_action_date,
  index_name,
  avg(100.0*index_date_action)as repeat_rate
from  
  user_action_flag
GROUP BY 
  first_action_date,index_name
order by first_action_date,index_name



---------------------------------------------------------

1000073953.1573574894

