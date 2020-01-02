with
t_users AS(
  SELECT
    Users,
    max(OMO_Members) as OMO_ID ,
    max(User_Gender) as Gender_1,
    max(User_DOB) as Age_1,
    sum(case when Content_Category = "娛樂" then 1 end) as A,
    sum(case when Content_Category = "社會" then 1 end) as B,
    sum(case when Content_Category = "生活" then 1 end) as C,
    sum(case when Content_Category = "國際" then 1 end) as D,
    sum(case when Content_Category = "政治" then 1 end) as E,
    sum(case when Content_Category = "財經" then 1 end) as F,
    sum(case when Content_Category = "頭條要聞" then 1 end) as G,
    sum(case when Content_Category = "論壇" then 1 end) as H,
    sum(case when Content_Category = "微視蘋" then 1 end) as I,
    sum(case when Content_Category = "體育" then 1 end) as J,
    sum(case when Content_Category = "副刊" then 1 end) as K,
    sum(case when Content_Category = "時尚" then 1 end) as L,
    sum(case when Content_Category = "3C" then 1 end) as M,
    sum(case when Content_Category = "地產" then 1 end) as N,
    sum(case when Content_Category = "車市" then 1 end) as O,
    sum(case when Content_Category = "生活家庭" then 1 end) as P,
    sum(case when Content_Category = "生活潮流" then 1 end) as Q,
    sum(case when Content_Category = "動物" then 1 end) as R,
    sum(case when Content_Category = "地產王" then 1 end) as S,
    sum(case when Content_Category = "蘋果國際" then 1 end) as T,
    sum(case when Content_Category = "名采人間事" then 1 end) as U,
    sum(case when Content_Category = "生活科技" then 1 end) as V,
    sum(case when Content_Category = "家居王" then 1 end) as W,
    sum(case when Content_Category = "搜奇" then 1 end) as X,
    sum(case when Content_Category = "勾奇" then 1 end) as Y,
    sum(case when Content_Category = "副刊廣編" then 1 end) as Z,
    sum(case when Content_Category = "暖新聞" then 1 end) as AA,
    sum(case when Content_Category = "豪宅與中古" then 1 end) as BB,
    sum(case when Content_Category = "壹週刊" then 1 end) as CC,
    sum(case when Content_Category = "要聞" then 1 end) as DD
from
  `ga360-203507.sample_data_GA.action_log` 
group by Users
)

,platform as(
  select
     OMO_Members,
     count(distinct Platform) as Count_Platform
 from
   `ga360-203507.sample_data_GA.action_log` 
 group by OMO_Members
)

,User_platform as(
  select
    u.Users,
    u.OMO_ID,
    u.Gender_1,
    u.Age_1,
    u.A,
    u.B,
    u.C,
    u.D,
    u.E,
    u.F,
    u.G,
    u.H,
    u.I,
    u.J,
    u.K,
    u.L,
    u.M,
    u.N,
    u.O,
    u.P,
    u.Q,
    u.R,
    u.S,
    u.T,
    u.U,
    u.V,
    u.W,
    u.X,
    u.Y,
    u.Z,
    u.AA,
    u.BB,
    u.CC,
    u.DD,
    p.Count_Platform  as Count_Platform 
  from
    t_users as u 
  left outer join
    platform as p
  on u.OMO_ID=p.OMO_Members
)
,count_sub as(
  select
    Account__Account_Number,
    max(Rate_Plan_Charge__Created_Date) as Subscribe_time,
    count(Amendment__Type) as Count_sub
FROM
    `nd-data-poc.zuora.ZuoraSubscribe`
group by Account__Account_Number   
)

,count_unsub as(
  select
    Account__Account_Number,
    max(Rate_Plan_Charge__Created_Date) as Unsubscribe_time,
    count(Amendment__Type) as Count_unsub
 FROM
    `nd-data-poc.zuora.ZuoraUnsubscribe`
group by Account__Account_Number  
)

,combine_Subscribers AS(
  SELECT
    ZS.Account__Account_Number,
    Subscribe_time,
    Unsubscribe_time,
    Count_sub,
    Count_unsub,
    case 
    when coalesce(Count_sub,0)-coalesce(Count_unsub,0) > 0 then"升級壹會員"
    when coalesce(Count_sub,0)-coalesce(Count_unsub,0) <= 0 then"取消訂閱壹會員"
    end as User_type_1
  FROM
    count_sub ZS
  left outer JOIN
    count_unsub ZU
  ON
    ZS.Account__Account_Number=ZU.Account__Account_Number)

select
  u.Users,
  u.OMO_ID,
  c.Account__Account_Number,
  u.Gender_1,
  case 
    when Gender_1="1" then "Male"
    when Gender_1="2" then "Female"
  end as Gender,
  u.Age_1,
  case
    when Age_1="1" then "<13"
    when Age_1="2" then "13-17"
    when Age_1="3" then "18-24"
    when Age_1="4" then "25-34"
    when Age_1="5" then "35-44"
    when Age_1="6" then "45-54"
    when Age_1="7" then "55-64"
    when Age_1="8" then ">65"
  end as Age,
  u.A,
  u.B,
  u.C,
  u.D,
  u.E,
  u.F,
  u.G,
  u.H,
  u.I,
  u.J,
  u.K,
  u.L,
  u.M,
  u.N,
  u.O,
  u.P,
  u.Q,
  u.R,
  u.S,
  u.T,
  u.U,
  u.V,
  u.W,
  u.X,
  u.Y,
  u.Z,
  u.AA,
  u.BB,
  u.CC,
  u.DD,
  u.Count_Platform,
  c.Subscribe_time,
  c.Unsubscribe_time,
  c.Count_sub,
  c.Count_unsub,
  c.User_type_1,
  case 
    when User_type_1="升級壹會員" then"升級壹會員"
    when User_type_1="取消訂閱壹會員" then"取消訂閱壹會員"
    when regexp_contains(OMO_ID, '^5') then "已登入OMO會員" else"未登入使用者"   
  end as User_type
from 
  User_platform as u 
left outer join
  combine_Subscribers as c
on
  u.OMO_ID = c.Account__Account_Number
order by Count_Platform desc
