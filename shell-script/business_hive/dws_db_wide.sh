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

with t1 as
(
    select
        user_id,
        count(*) order_count,
        sum(total_amount) order_amount,
        0 payment_count,
        0 payment_amount,
        0 comment_count
    from dwd_order_info
    where dt='$do_date'
    group by user_id
),
t2 as
(
    select
        user_id,
        0 order_count,
        0 order_amount,
        count(*) payment_count,
        sum(total_amount) payment_amount,
        0 comment_count
    from dwd_payment_info
    where dt='$do_date'
    group by user_id
),

t3 as
(
    select
        user_id,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount,
        count(*) comment_count
    from dwd_comment_log
    where dt='$do_date'
    group by user_id
)

insert overwrite table dws_user_action partition(dt='$do_date')
select
    user_id,
    sum(order_count),
    sum(order_amount),
    sum(payment_count),
    sum(payment_amount),
    sum(comment_count)
from(
    select * from t1
    union all
    select * from t2
    union all
    select * from t3
)t3
group by user_id

"

$hive -e "$sql"
