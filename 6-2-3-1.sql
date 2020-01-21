WITH
  combine_sub as(
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
      NUll as action,
    FROM `ga360-203507.sample_data_GA.action_log`
      
      union all
   
   select  
      NULL as Users,
      Account__Account_Number as OMO_Members,
      NULL as Sessions,
      NULL as Sessions_count,
      NULL as bounces,
      Rate_Plan_Charge__Created_Date as timestamp,
      NULL as Content_Category,
      NULL as Content_Title,
      NULL as Event_Category,
      NULL as Event_Action,
      NULL as Event_Label,
      NULL as hit_type,
      NULL as hits_count,
      NULL as hitnumber,
      NULL as pagepath,
      "Subscribe" as action       
    FROM `nd-data-poc.zuora.ZuoraSubscribe` )

select *
from  combine_sub
where OMO_Members	="5cae0115d0da610295bb58c7"
order by timestamp