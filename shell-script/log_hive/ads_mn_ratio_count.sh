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
insert into table ads_mn_ratio_count
select
    '$do_date',
    date_format('$do_date','yyyy-MM'),
    mn_count/sum_user*100 mn_percent
from
(select count(*) mn_count from dws_uv_detail_mn where mn=date_format('$do_date','yyyy-MM')) t1,
(select sum(new_mid_count) sum_user from ads_new_mid_count) t2;
"

$hive -e "$sql"
