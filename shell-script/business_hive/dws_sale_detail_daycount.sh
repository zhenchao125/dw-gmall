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
-- 每天每个用户购买每个产品的次数
with tmp_detail as(
select
  user_id,
  sku_id,
  sum(sku_num) sku_num, -- 购买个数
  count(*) order_count, -- 订单个数
  sum(order_price * sku_num) order_amount-- 订单额度
from dwd_order_detail
where dt='$do_date'
group by user_id, sku_id
)

insert overwrite table dws_sale_detail_daycount
partition(dt='$do_date')
select
  tmp_detail.user_id,
  tmp_detail.sku_id,
  dui.gender,
  months_between('$do_date', dui.birthday)/12  age, -- 方便获得新分析不同年龄段的人的购买行为
  dui.user_level,
  price,
  sku_name,
  tm_id,
  category3_id,
  category2_id,
  category1_id,
  category3_name,
  category2_name,
  category1_name,
  spu_id,
  tmp_detail.sku_num,
  tmp_detail.order_count,
  tmp_detail.order_amount
from tmp_detail
join dwd_user_info dui
join dwd_sku_info dsi
on tmp_detail.user_id=dui.id and tmp_detail.sku_id=dsi.id
where dui.dt='$do_date' and dsi.dt='$do_date';
"

$hive -e "$sql"
