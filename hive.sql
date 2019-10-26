insert into ads_user_convert_day
select
  '2019-09-02',
  sum(day_count),
  sum(new_mid_count),
  sum(new_mid_count) / sum(day_count) * 100
from
(select
  0 day_count,
  new_mid_count
from ads_new_mid_count
where create_date='2019-09-02'
union
select
  day_count,
  0 new_mid_count
from ads_uv_count
where dt='2019-09-02') uv
