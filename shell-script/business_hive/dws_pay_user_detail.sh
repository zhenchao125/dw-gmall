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
insert overwrite table dws_pay_user_detail partition(dt='$do_date')
select
   ua.user_id,
   ui.name,
   ui.birthday,
   ui.gender,
   ui.email,
   ui.user_level
from (
  select user_id from dws_user_action where dt='$do_date'
) ua join(
  select * from dwd_user_info where dt='$do_date'
) ui on ua.user_id=ui.id
left join dws_pay_user_detail ud on ua.user_id=ud.user_id
where ud.user_id is null;
"

$hive -e "$sql"
