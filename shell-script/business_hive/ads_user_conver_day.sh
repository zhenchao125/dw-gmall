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
insert into ads_user_convert_day
select
  '$do_date',
  sum(day_count),
  sum(new_mid_count),
  sum(new_mid_count) / sum(day_count) * 100
from
(select
  0 day_count,
  new_mid_count
from ads_new_mid_count
where create_date='$do_date'
union
select
  day_count,
  0 new_mid_count
from ads_uv_count
where dt='$do_date') uv
"

$hive -e "$sql"
