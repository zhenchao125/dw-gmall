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
insert overwrite table ads_user_total_count partition(dt='$do_date')
select
  if(today.mid_id is null, yesterday.mid_id, today.mid_id) mid_id,
  today.subtotal,
  if(today.subtotal is null , 0, today.subtotal) + if(yesterday.total is null, 0, yesterday.total) total
from (
  select
    *
  from dws_user_total_count_day
  where dt='$do_date'
) today
full join (
  select
    *
  from ads_user_total_count
  where dt=date_add('$do_date', -1)
) yesterday
on today.mid_id=yesterday.mid_id
"

$hive -e "$sql"
