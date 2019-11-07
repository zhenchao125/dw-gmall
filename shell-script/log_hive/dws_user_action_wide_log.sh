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
insert overwrite table dws_user_action_wide_log partition(dt='$do_date')
select
    mid_id,
    goodsid,
    sum(display_count) display_count,
    sum(praise_count) praise_count,
    sum(favorite_count) favorite_count
from
( select
        mid_id,
        goodsid,
        count(*) display_count,
        0 praise_count,
        0 favorite_count
    from
        dwd_display_log
    where
        dt='$do_date'
    group by
        mid_id,goodsid

    union all

    select
        mid_id,
        target_id goodsid,
        0,
        count(*) praise_count,
        0
    from
        dwd_praise_log
    where
        dt='$do_date'
    group by
        mid_id,target_id

    union all

    select
        mid_id,
        course_id goodsid,
        0,
        0,
        count(*) favorite_count
    from
        dwd_favorites_log
    where
        dt='$do_date'
    group by
        mid_id,course_id
)user_action
group by
mid_id,goodsid;

"

$hive -e "$sql"
