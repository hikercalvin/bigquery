with action_log_with_status as(
  select
    sessions
   ,Users
   ,hit_type
   ,Registered_Session
   ,case when Registered_Session="YES" then "login" else "guest" end as login_status 
  from
    `ga360-203507.sample_data_GA.4_2`
  )
  
  select
    *
  from
    action_log_with_status