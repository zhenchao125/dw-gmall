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

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_order_info partition(dt)
select * from ods_order_info 
where dt='$do_date' and id is not null;
 
insert overwrite table dwd_order_detail partition(dt)
select * from ods_order_detail 
where dt='$do_date'  and id is not null;

insert overwrite table dwd_user_info partition(dt)
select * from ods_user_info
where dt='$do_date' and id is not null;
 
insert overwrite table dwd_payment_info partition(dt)
select * from ods_payment_info
where dt='$do_date' and id is not null;

insert overwrite table dwd_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from ods_sku_info sku
join ods_base_category3 c3 on sku.category3_id=c3.di
join ods_base_category2 c2 on c3.category2_id=c2.id
join ods_base_category1 c1 on c2.category1_id=c1.id
where sku.dt='$do_date' and c3.dt='$do_date'
and c2.dt='$do_date' and c1.dt='$do_date'
adn sku.id is not null
"

$hive -e "$sql"
