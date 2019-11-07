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
insert into table ads_goods_count
select
    '$do_date',
    goodsid,
    mid_id,
    sum_display_count
from(
    select
      goodsid,
      mid_id,
      sum_display_count,
      row_number() over(partition by goodsid order by sum_display_count desc) rk
    from(
      select
        goodsid,
        mid_id,
        sum(display_count) sum_display_count
      from dws_user_action_wide_log
      where display_count>0
      group by goodsid, mid_id
    ) t1
) t2
where rk <= 3
"
$hive -e "$sql"
