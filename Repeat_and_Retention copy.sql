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
    date_add(cast(u.first_action_date as date),interval 1 day) as next_day_1,
    date_add(cast(u.first_action_date as date),interval 2 day) as next_day_2,
    date_add(cast(u.first_action_date as date),interval 3 day) as next_day_3,
    date_add(cast(u.first_action_date as date),interval 4 day) as next_day_4,
    date_add(cast(u.first_action_date as date),interval 5 day) as next_day_5,
    date_add(cast(u.first_action_date as date),interval 6 day) as next_day_6,
    date_add(cast(u.first_action_date as date),interval 7 day) as next_day_7,
    date_add(cast(u.first_action_date as date),interval 8 day) as next_day_8,
    date_add(cast(u.first_action_date as date),interval 9 day) as next_day_9,
    date_add(cast(u.first_action_date as date),interval 10 day) as next_day_10,
    date_add(cast(u.first_action_date as date),interval 11 day) as next_day_11,
    date_add(cast(u.first_action_date as date),interval 12 day) as next_day_12,
    date_add(cast(u.first_action_date as date),interval 13 day) as next_day_13,
    date_add(cast(u.first_action_date as date),interval 14 day) as next_day_14
from visit_number as u
  left outer join
  `ga360-203507.sample_data_GA.action_log` as a
on u.Users = a.Users
)
,user_action_flag as(
  select
    Users,
    first_action_date,
    latest_date,
    sign(
      sum(
        case when next_day_1 <= latest_date then
          case when next_day_1 = action_date then 1 else 0 end
         end
        )
       )as next_1_day_action,
     
     sign(
      sum(
        case when next_day_2 <= latest_date then
          case when next_day_2 = action_date then 1 else 0 end
         end
        )
       )as next_2_day_action,
      
      sign(
       sum(
        case when next_day_3 <= latest_date then
          case when next_day_3 = action_date then 1 else 0 end
         end
        )
       )as next_3_day_action,
   
     sign(
      sum(
        case when next_day_4 <= latest_date then
          case when next_day_4 = action_date then 1 else 0 end
         end
        )
       )as next_4_day_action,
       
       sign(
          sum(
        case when next_day_5 <= latest_date then
          case when next_day_5 = action_date then 1 else 0 end
         end
        )
       )as next_5_day_action,
       
     sign(
      sum(
        case when next_day_6 <= latest_date then
          case when next_day_6 = action_date then 1 else 0 end
         end
        )
       )as next_6_day_action,
       
       sign(
        sum(
        case when next_day_7 <= latest_date then
          case when next_day_7 = action_date then 1 else 0 end
         end
        )
       )as next_7_day_action,
     
     sign(
      sum(
        case when next_day_8 <= latest_date then
          case when next_day_8 = action_date then 1 else 0 end
         end
        )
       )as next_8_day_action,
       
        sign(
         sum(
        case when next_day_9 <= latest_date then
          case when next_day_9 = action_date then 1 else 0 end
         end
        )
       )as next_9_day_action,
     
     sign(
      sum(
        case when next_day_10 <= latest_date then
          case when next_day_10 = action_date then 1 else 0 end
         end
        )
       )as next_10_day_action,
      
      sign(
        sum(
        case when next_day_11 <= latest_date then
          case when next_day_11 = action_date then 1 else 0 end
         end
        )
       )as next_11_day_action,
     
     sign(
      sum(
        case when next_day_12 <= latest_date then
          case when next_day_12 = action_date then 1 else 0 end
         end
        )
       )as next_12_day_action,
       
      sign(
       sum(
        case when next_day_13 <= latest_date then
          case when next_day_13 = action_date then 1 else 0 end
         end
        )
       )as next_13_day_action,
     
     sign(
      sum(
        case when next_day_14 <= latest_date then
          case when next_day_14 = action_date then 1 else 0 end
         end
        )
       )as next_14_day_action
  from
    action_log_with_mst_users
  group by 
    Users,
    first_action_date,
    latest_date
)
select
  *
from  
  user_action_flag
order BY 
  first_action_date, Users



