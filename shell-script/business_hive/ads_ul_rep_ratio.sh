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
with
tmp_count as(
  select -- 每个等级每个产品的下单次数
    user_level,
    sku_id,
    sum(order_count) order_count
  from dws_sale_detail_daycount
  where dt<='$do_date' and dt>='2019-10-03'
  group by user_level, sku_id
)
insert overwrite table ads_ul_rep_ratio
select
  *
from(
  select
    user_level,
    sku_id,
    sum(if(order_count >=1, 1, 0)) buy_count,
    sum(if(order_count >=2, 1, 0)) buy_twice_count,
    sum(if(order_count >=2, 1, 0)) / sum(if(order_count >=1, 1, 0)) * 100  buy_twice_rate,
    row_number() over(partition by user_level order by sum(if(order_count >=2, 1, 0)) / sum(if(order_count >=1, 1, 0)) desc) rn,
    '$do_date'
  from tmp_count
  group by user_level, sku_id
) t1
where rn<=10
"

$hive -e "$sql"
