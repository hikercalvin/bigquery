SELECT
   date
  ,substr(date, 1, 6) as year_month
  ,sum(Total_Content_View) as Total_Content_View
  ,sum(Total_Video_View) as Total_Video_View
  ,sum(sum(Total_Content_View)) over(partition by substr(date,1,6) order by date rows unbounded preceding) as agg_Content_View
  ,sum(sum(Total_Video_View)) over(partition by substr(date,1,6) order by date rows unbounded preceding) as agg_Video_View
 
FROM `ga360-203507.sample_data_GA.4_1_subscribers` 
group by date
order by date