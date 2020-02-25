WITH
  activity_log_with_conversion_flag as (
    select
      Users,
      OMO_Members,
      Sessions,
      Sessions_count,
      bounces,
      timestamp,
      Content_Category,
      Content_Title,
      Event_Category,
      Event_Action,
      Event_Label,
      hit_type,
      hits_count,
      hitnumber,
      pagepath,
      action,
      sign(sum(case when action ="Subscribe" then 1 else 0 end) over(partition by OMO_Members order by timestamp desc
      rows between unbounded preceding and current row))
     as has_conversion
     FROM `ga360-203507.sample_data_GA.action_log`
     where Content_Title is not null 
     and regexp_contains(hit_type,'PAGE|APPVIEW'))
      
select 
  *
from  activity_log_with_conversion_flag
order by timestamp desc