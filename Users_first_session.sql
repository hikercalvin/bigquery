with visit_number as(
  select 
   Users,
   sum(Content_View) as Total_Content_View,
   count(distinct Content_ID) as Count_article,
   min(timestamp) as first_action_date,
   Max(Sessions) as Sessions,
   Max(total_timeOnSite) as Session_Duration,
   Sessions_count
  from `ga360-203507.sample_data_GA.action_log`
  where Sessions_count=1
  group by
   Users,
   Sessions_count)

select
  *
from  
  visit_number
order BY 
  first_action_date, Users

