WITH
  activity_log_with_landing_exit as (
  select
     Users,
     Sessions,
     timestamp,
     Content_Category,
     hitnumber,
     First_Value(Content_Title)
      over(
        partition by Sessions
        order by hitnumber asc
          rows between unbounded preceding
            and unbounded following
         ) as landing,
      Last_Value(Content_Title)
        over(
          partition by Sessions
          order by hitnumber asc
            rows between unbounded preceding
              and unbounded following
           ) as exit
    FROM `ga360-203507.sample_data_GA.action_log`
    where Content_Title is not null
)

select *
from  activity_log_with_landing_exit
order by timestamp, hitnumber