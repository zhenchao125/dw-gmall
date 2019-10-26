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
insert into table ads_sale_tm_category1_stat_mn
select -- 统计用户个数
  sku_id,
  sku_category1_id,
  sku_category1_name,
  sum(if(sum_order_count>=1,1,0)),  -- 购买一次的人数
  sum(if(sum_order_count>=2,1,0)),  -- 购买两次的人数
  sum(if(sum_order_count>=2,1,0)) / sum(if(sum_order_count>=1,1,0)) * 100,  -- 单次复购率
  sum(if(sum_order_count>=3,1,0)),  -- 购买三次的人数
  sum(if(sum_order_count>=3,1,0)) / sum(if(sum_order_count>=1,1,0)) * 100,  -- 多次复购率
  date_format('2019-02-10' ,'yyyy-MM') stat_mn,
  '2019-02-10' stat_date
from(
select  -- 每用户, 每个品牌,每个一级品类的购买次数 (当月)
  sku_id,
  sku_category1_id,
  sku_category1_name,
  sum(order_count) sum_order_count
from dws_sale_detail_daycount
where date_format(dt, 'yyyy-MM')=date_format('2019-09-02', 'yyyy-MM')
group by user_id, sku_id, sku_category1_id, sku_category1_name
)tmp
group by sku_id,sku_category1_id, sku_category1_name;
"

$hive -e "$sql"
