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
insert into ads_user_retention_day_count
select
    create_date,
    retention_day,
    count(*) retention_count
from dws_user_retention_day
where dt='$do_date'
group by create_date, retention_day;

insert into ads_user_retention_day_rate
select
    $do_date,
    ur.create_date,
    ur.retention_day,
    ur.retention_count,
    nm.new_mid_count,
    ur.retention_count / nm.new_mid_count * 100
from ads_user_retention_count_day ur join ads_new_mid_count nm
on ur.create_date=nm.create_date
where date_add(ur.create_date, ur.retention_day)='$do_date'
"
$hive -e "$sql"
