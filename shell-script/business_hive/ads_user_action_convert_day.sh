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
insert into ads_user_action_convert_day
select
  do_dt,
  day_count , -- 日活
  order_u_count,  -- 下单人数
  order_u_count / day_count, -- 下单转化率
  payment_u_count,
  payment_u_count / day_count
from(
  select
    '$do_date' do_dt,
    sum(if(order_count>0,1,0)) order_u_count,  -- 下单人数
    sum(if(payment_count>0,1,0)) payment_u_count  -- 支付人数
  from dws_user_action
  where dt='$do_date'
) tmp join ads_uv_count auc
on tmp.do_dt=auc.dt
limit 1;
"

$hive -e "$sql"
