with 
  sub as(
select
    date(Rate_Plan_Charge__Created_Date) as Sub_Created_Date,
    count(distinct Account__Account_Number) as Subscribers
FROM
    `nd-data-poc.zuora.ZuoraSubscribe` 
where
  regexp_contains(Product_Rate_Plan__Name, '^Taiwan Apple Daily')
group by Sub_Created_Date
)

, cumulative as(
select
    Sub_Created_Date,
    Subscribers,
    sum(Subscribers)over(order by Sub_Created_Date rows between unbounded preceding and current row) as Count_Cumulative,
    Subscribers/sum(Subscribers)over(order by Sub_Created_Date rows between unbounded preceding and current row) as Cumulative_Rate,
    case 
      when
        7=count(*)
          over(order by Sub_Created_Date rows between 6 preceding and current row)
      then
        avg(Subscribers)over(order by Sub_Created_Date rows between 6 preceding and current row)
     end
     as Seven_day_avg_strict
from
    sub
)
select
  *
from cumulative
order by Sub_Created_Date

