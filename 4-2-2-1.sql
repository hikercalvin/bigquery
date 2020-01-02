with 
sales_composition_ratio as (
  select
    Navigation_Channel
   ,amount_Video_View
   ,100.0 * amount_Video_View / sum( amount_Video_View) over() as composition_ration
   ,100.0 * sum( amount_Video_View )over(order by  amount_Video_View desc) / sum( amount_Video_View ) over() as cumulative_ratio
from `ga360-203507.sample_data_GA.4_2_2`
)

select
 *
 , case
    when cumulative_ratio between 0 and 70 then 'A'
    when cumulative_ratio between 70 and 90 then 'B'
    when cumulative_ratio between 90 and 100 then 'C'
  end as abc_rank
from 
  sales_composition_ratio
order by amount_Video_View desc