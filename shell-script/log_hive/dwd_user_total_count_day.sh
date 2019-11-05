#!/bin/bash

db=gmall
hive=/opt/module/hive-1.2.1/bin/hive
hadoop=/opt/module/hadoop-2.7.2/bin/hadoop

if [[ -n $1 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi
# 每日登陆次数统计
sql="
use gmall;
insert overwrite table dws_user_total_count_day
partition(dt='$do_date')
select
    mid_id,
    count(mid_id) cm
from dwd_start_log
where dt='$do_date'
group by mid_id;
"

$hive -e "$sql"
