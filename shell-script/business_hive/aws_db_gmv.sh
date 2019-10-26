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
insert into table ads_gmv_sum_day
select
  '$do_date',
  count(*),
  sum(ua.order_amount),
  sum(payment_amount)
from dws_user_action ua
where dt='$do_date'
"

$hive -e "$sql"
