SELECT 
   date
  ,count(*) as Sessions_count
  ,sum(Total_Content_View) as Total_Content_View
  ,sum(Total_Video_View) as Total_Video_View
  ,avg(Total_Content_View) as avg_Content_View
  ,avg(Total_Video_View) as avg_Video_View

FROM `ga360-203507.sample_data_GA.4_1_subscribers` 
group by date
order by date