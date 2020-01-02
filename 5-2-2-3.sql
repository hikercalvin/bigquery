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
    max(date(a.timestamp)) over() as latest_date,
    date_add(cast(u.first_action_date as date ), interval 1 day) as next_day_1
from visit_number as u
  left outer join
  `ga360-203507.sample_data_GA.action_log` as a
on u.Users = a.Users
)

, user_action_flag as(
  select
    Users,
    first_action_date,
    sign(
      sum(
        case when 	next_day_1 <= latest_date then
          case when next_day_1 = action_date then 1 else 0 end
         end
         )
        ) as next_1_day_action
    from
      action_log_with_mst_users
    group by
      Users, first_action_date
)
select  
    first_action_date,
    avg(100.0 * next_1_day_action) as repeat_rate_1_day
  from user_action_flag 
  group by first_action_date
  order by first_action_date
