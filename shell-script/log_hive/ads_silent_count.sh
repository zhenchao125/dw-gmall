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

insert into ads_silent_count
select
    '$do_date',
    count(*)
from(
    select
        mid_id
    from dws_uv_detail_day
    group by mid_id
    where dt<=$do_date
    having count(*)=1
    and max(dt)<=date_add('$do_date', -7)
) t1
"

$hive -e "$sql"


