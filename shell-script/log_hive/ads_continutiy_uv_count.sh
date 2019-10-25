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

insert into table ads_continuity_uv_count

select
    '$do_date',
    concat(date_add('$do_date',-6),'_','$do_date'),
    count(*)
from(
    select
        mid_id
    from(
        select
            mid_id
        from(
            select
                mid_id,
                dt,
                rank() over(partition by mid_id order by dt) rk
            from dws_uv_detail_day
            where dt >=date_add('$do_date', -6)
        ) t1
        group by mid_id, date_add(dt, -rk)
        having count(*)>=3
    )t2
    group by mid_id
)t3
"

下面是注释: 
-------------------------
$hive -e "$sql"



mid_id          dt              rank    date_diff
1               2019-09-01      1       2019-08-31
1               2019-09-02      2       2019-08-31
1               2019-09-03      2       2019-08-31

1               2019-09-05      3       2019-09-02
1               2019-09-06      4       2019-09-02
1               2019-09-07      5       2019-09-02
1               2019-09-08      6       2019-09-02