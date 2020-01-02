select
   coalesce(Navigation_Channel, 'all') as Navigation_Channel
  ,coalesce(Content_Title, 'all') as Content_Title
  ,sum( Content_View ) as amount_Content_View
  ,sum( Video_View ) as amount_Video_View
FROM `ga360-203507.sample_data_GA.4_2`
    group by Rollup(Navigation_Channel, Content_Title)