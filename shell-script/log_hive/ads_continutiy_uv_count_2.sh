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
    '2019-09-11',
    concat(date_add('2019-09-11',-6),'_','2019-09-11'),
    count(*)
from(
    select
        distinct(mid_id)
    from(
        select
            mid_id,
            dt,
            lead(dt, 2, '9999-12-31') over(partition by mid_id order by dt) lead2
        from dws_uv_detail_day
        where dt>=date_add('2019-09-10', -6)
    )t1
    where datediff(lead2, dt)=2
)t2
"

下面是注释: 
-------------------------
$hive -e "$sql"



mid_id          dt              lead2            date_diff
1               2019-09-01      2019-09-03          2
1               2019-09-02      2019-09-05          3
1               2019-09-03      2019-09-06          3

1               2019-09-05      2019-09-07          2
1               2019-09-06      2019-09-08          2
1               2019-09-07      9999-12-12          ...
1               2019-09-08      9999-12-12          ...