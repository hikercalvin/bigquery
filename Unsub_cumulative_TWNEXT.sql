with 
  unsub as(
select
    date(Rate_Plan_Charge__Created_Date) as Unsub_Created_Date,
    count(distinct Account__Account_Number) as Unsubscribers
FROM
    `nd-data-poc.zuora.ZuoraUnsubscribe`
where
  regexp_contains(Product_Rate_Plan__Name, '^Taiwan Next Magazine')
group by Unsub_Created_Date
)

, cumulative as(
select
    Unsub_Created_Date,
    Unsubscribers,
    sum(Unsubscribers)over(order by Unsub_Created_Date rows between unbounded preceding and current row) as Count_Cumulative,
    Unsubscribers/sum(Unsubscribers)over(order by Unsub_Created_Date rows between unbounded preceding and current row) as Cumulative_Rate,
    case 
      when
        7=count(*)
          over(order by Unsub_Created_Date rows between 6 preceding and current row)
      then
        avg(Unsubscribers)over(order by Unsub_Created_Date rows between 6 preceding and current row)
     end
     as Seven_day_avg_strict
from
    unsub
)
select
  *
from cumulative
order by Unsub_Created_Date

