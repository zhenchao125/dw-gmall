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
insert into table ads_goods_display_top10
select
  '$do_date',
  category,
  goodsid,
  count
from(
  select
    category,
    goodsid,
    count,
    rank() over(partition by category order by count desc) rk
  from(
    select
      category,
      goodsid,
      count(*) count
    from dwd_display_log
    where dt='$do_date' and action=2
    group by category, goodsid
  )t1
)t2
where rk<=10
"

$hive -e "$sql"
