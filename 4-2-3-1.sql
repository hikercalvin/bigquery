with
daily_category_amount as (
  select
    date
    ,Navigation_Channel
    ,sum(Video_View)as amount_Video_View
 from `ga360-203507.sample_data_GA.4_2`
 group by date, Navigation_Channel
)

select
   date
  ,Navigation_Channel
  ,amount_Video_View
  ,First_Value(amount_Video_View) over(partition by Navigation_Channel order by date, Navigation_Channel rows unbounded preceding) as base_amount
  ,100.0*amount_Video_View/First_Value(amount_Video_View) over(partition by Navigation_Channel order by date, Navigation_Channel rows unbounded preceding) as rate
from  daily_category_amount
order by date, Navigation_Channel