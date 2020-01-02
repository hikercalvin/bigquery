with
sub_category_amount as(
  select 
      Navigation_Channel
    , Content_Title
    ,sum( Content_View ) as amount_Content_View
    ,sum( Video_View ) as amount_Video_View
FROM `ga360-203507.sample_data_GA.4_2`
    group by Navigation_Channel, Content_Title
)
, category_amount as(
  select
      Navigation_Channel
     ,'all' as Content_Title
     ,sum( Content_View ) as amount_Content_View
     ,sum( Video_View ) as amount_Video_View
 FROM `ga360-203507.sample_data_GA.4_2`
    group by Navigation_Channel
)
,total_amount as (
  select
   'all' as Navigation_Channel
  ,'all' as Content_Title
  ,sum( Content_View ) as amount_Content_View
  ,sum( Video_View ) as amount_Video_View
 FROM `ga360-203507.sample_data_GA.4_2`
 )
 
select Navigation_Channel,Content_Title,amount_Content_View,amount_Video_View from sub_category_amount
union all select Navigation_Channel,Content_Title,amount_Content_View,amount_Video_View from category_amount
union all select Navigation_Channel,Content_Title,amount_Content_View,amount_Video_View from total_amount