#!/bin/bash

db=gmall
hive=/opt/module/hive-1.2.1/bin/hive
hadoop=/opt/module/hadoop-2.7.2/bin/hadoop

if [[ -n $1 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

sql="
use gmall;
insert into table ads_goods_user_count
select
  '$do_date',
  mid_id,
  u_ct,
  goodsid,
  d_ct
from(
  select
    mid_id,
    u_ct,
    goodsid,
    d_ct,
    row_number() over(partition by mid_id order by d_ct desc ) rn
  from(
    select
      dl.mid_id,
      u_ct,
      dl.goodsid,
      count(*) d_ct
    from dwd_display_log dl join (
      select
        mid_id,
        count(*) u_ct
      from dws_user_action_wide_log
      group by mid_id
      order by u_ct desc
      limit 10
    )t1
    on dl.mid_id=t1.mid_id
    where dt>='2019-10-03' and dt<='2019-10-31'
    group by dl.mid_id, u_ct, dl.goodsid
  ) t2
) t3
where rn<=10
"

$hive -e "$sql"
