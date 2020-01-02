with 
  unique_action_log as (
    select 
    distinct Users as User_ID,
    substr(string(timestamp), 1, 10) as action_date
  from `ga360-203507.sample_data_GA.action_log`
  
)

,mst_calender as(
  select date_add(min(date(timestamp(action_date))), interval 1 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 2 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 3 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 4 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 5 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 6 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 7 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 8 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 9 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 10 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 11 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 12 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 13 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 14 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 15 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 16 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 17 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 18 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 19 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 20 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 21 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 22 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 23 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 24 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 25 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 26 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 27 day) as dt
  from unique_action_log
  union all select date_add(min(date(timestamp(action_date))), interval 28 day) as dt
  from unique_action_log
)

,target_date_with_user as(
  select
    c.dt as target_date,
    u.Users as User_ID,
    substr(string(u.Subscribe_time), 1, 10) as Subscribe_date,
    substr(string(u.Unsubscribe_time), 1, 10) as Unsubscribe_date
from
  `ga360-203507.sample_data_GA.mst_users`as u
cross join
  mst_calender as c
)

,user_status_log as(
  select 
    u.target_date,
    u.User_ID,
    u.Subscribe_date,
    u.Unsubscribe_date,
    a.action_date,
    case when u.Subscribe_date = a.action_date then 1 else 0 end is_new,
    case when u.Unsubscribe_date = a.action_date then 1 else 0 end is_exit,
    case when u.target_date = date(timestamp(a.action_date)) then 1 else 0 end is_access,
    lag(case when u.target_date = date(timestamp(a.action_date)) then 1 else 0 end) over(partition by u.User_ID order by u.target_date) as was_access
  from 
    target_date_with_user as u
  left outer join
    unique_action_log as a
    on u.User_ID = a.User_ID
    and u.target_date = date(timestamp(a.action_date))
  where 
    date(timestamp(u.Subscribe_date)) <= u.target_date
    and( u.Unsubscribe_date is NULL
    or u.target_date <= date(timestamp(u.unsubscribe_date)))
) 

,user_growth_index as(
  select
    *,
   case 
    when is_new + is_exit =1 then 
      case  
        when is_new =1 then 'signup'
        when is_exit =1 then 'exit'
      end
     when is_new + is_exit =0 then
      case
        when was_access =0 and is_access =1 then 'reactivation'
        when was_access =1 and is_access =0 then 'deactivation'
      end
     end as growth_index   
from
  user_status_log
)

select
  *,
  case growth_index
    when 'signup' then 1 
    when 'reactivation' then 1
    when 'deactivation' then -1
    when 'exit' then -1
   else 0
  end
  as rank_growth_index
from user_growth_index
order by User_ID,target_date

