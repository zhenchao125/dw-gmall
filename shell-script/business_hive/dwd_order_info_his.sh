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
insert overwrite table dwd_order_info_his_tmp
select -- 新增和改变
    id,
    total_amount,
    order_status,
    user_id,
    payment_way,
    out_trade_no,
    create_time,
    operate_time,
    '$do_date',
    '9999-99-99'
from ods_order_info
where dt='$do_date'
union all
select -- 查出来更改的数据, 修改需要更改的数据
    oh.id,
    oh.total_amount,
    oh.order_status,
    oh.user_id,
    oh.payment_way,
    oh.out_trade_no,
    oh.create_time,
    oh.operate_time,
    oh.start_date,
    if(oi.id is null, oh.end_date, date_add(oi.dt, -1)) -- 如果是 null 表示今天没有修改订单, 所以闭链时间保持不变
                                                        -- 如果不是 null 表示今天有修改, 所以把闭链时间换成昨天
from dwd_order_info_his oh
left join ( -- 左边的全部保留
  select * from dwd_order_info where dt='$do_date'
) oi
on oh.id=oi.id and oh.end_date='9999-99-99';  -- his.end_date='9999-99-99'  这样的订单才有可能需要修改闭链事件

insert overwrite table dwd_order_info_his
select * from dwd_order_info_his_tmp;
"

$hive -e "$sql"
