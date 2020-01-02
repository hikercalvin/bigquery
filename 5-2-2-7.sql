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
            select '07 day retention' as index_name, 1 as interval_begin_date, 7 as interval_end_date
            union all select '14 day retention' as index_name, 8 as interval_begin_date, 14 as interval_end_date
            union all select  '21 day retention' as index_name, 15 as interval_begin_date, 21 as interval_end_date
            union all select  '28 day retention' as index_name, 22 as interval_begin_date, 28 as interval_end_date
  )
  
,action_log_with_index_date as(
  select 
    u.Users as Users,
    date(u.first_action_date) as first_action_date,
    date(a.timestamp) as action_date,
    max(date(a.timestamp)) over() as latest_date,
    r.index_name,
    date_add(cast(u.first_action_date as date ), interval r.interval_begin_date day) as index_begin_date,
    date_add(cast(u.first_action_date as date ), interval r.interval_end_date day) as index_end_date
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
        case when index_end_date <= latest_date then
          case when action_date between index_begin_date and index_end_date then 1 else 0 end
         end
         )
        ) as index_date_action
    from
      action_log_with_index_date
    group by
      Users, first_action_date, index_name, index_begin_date, index_end_date )

,register_action_flag as(
select 
  m.Users,
  count(distinct date(a.timestamp)) as dt_count

from 
  `ga360-203507.sample_data_GA.Repeat_and_Retention`  as m
left join
  `ga360-203507.sample_data_GA.action_log` as a
  on m.Users=a.Users
  and date(a.timestamp)
    between date_add(cast(m.first_action_date as date), interval 1 day)
      and date_add(cast(m.first_action_date as date), interval 8 day)
left join 
  user_action_flag as f
  on m.Users = f.Users
where 
  f.index_date_action is not null
group by
  m.users
  ,f.index_name
  ,f.index_date_action
)

select 
  *
from
  register_action_flag


---------------------------------------------------------

1000073953.1573574894

