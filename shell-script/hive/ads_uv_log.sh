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
use $db;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ads_uv_count
select
    day.dt,
    day_count,
    wk_count,
    mn_count,
    if('$do_date'=date_add(next_day('$do_date', 'mo'), -1), 'Y', 'N'),
    if('$do_date'=last_day('$do_date'), 'Y', 'N')
from(
    select
        '$do_date' dt,
        count(*) day_count
    from dws_uv_detail_day
    where dt='$do_date'
) day join (
    select
        '$do_date' dt,
        count(*) wk_count
    from dws_uv_detail_wk
    where wk_dt=concat(date_add(next_day('$do_date','MO'),-7), '_' , date_add(next_day('$do_date','MO'),-1))
) wk join (
    select
        '$do_date' dt,
        count(*) mn_count
    from dws_uv_detail_mn
    where mn=date_format('$do_date', 'yyyy-MM')
) mn
where day.dt=wk.dt and day.dt=mn.dt;
"
$hive -e "$sql"