SELECT
   date
  ,sum(Total_Content_View) as Total_Content_View
  ,sum(Total_Video_View) as Total_Video_View
  ,avg(sum(Total_Content_View)) over(order by date rows between 6 preceding and current row) as Seven_Day_Avg_Content_View
  ,avg(sum(Total_Video_View)) over(order by date rows between 6 preceding and current row) as Seven_Day_Avg_Video_View
  ,case
    when
      7 = count(*)over(order by date rows between 6 preceding and current row)
    then
      avg(sum(Total_Content_View)) over(order by date rows between 6 preceding and current row) 
      end as Seven_Day_Avg_Content_View_Strict
  ,case
    when
      7 = count(*)over(order by date rows between 6 preceding and current row)
    then
      avg(sum(Total_Video_View)) over(order by date rows between 6 preceding and current row) 
      end as Seven_Day_Avg_Video_View_Strict
       
FROM `ga360-203507.sample_data_GA.4_1_subscribers` 
group by date
order by date