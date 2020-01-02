with status as(
	select count(distinct Sessions) as Total_sessions
    from `ga360-203507.sample_data_GA.action_log`
)

select
  a.Navigation_Section,
  count(distinct a.Sessions) as action_sessions,
  count(a) as action_count,
  s.Total_sessions,
  100.0*count(distinct a.Sessions)/s.Total_sessions as usage_rate,
  1.0*count(a)/count(distinct a.Sessions) as count_per_user
from
 `ga360-203507.sample_data_GA.action_log` as a
cross join
  status as s
group by
	a.Navigation_Section,s.Total_sessions
