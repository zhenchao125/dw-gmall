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
insert overwrite table dws_user_retention_day partition(dt='$do_date')
select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    3 retention_day
from dws_new_mid_day nm
join dws_uv_detail_day ud
on nm.mid_id=ud.mid_id
where nm.create_date=date_add('$do_date', -3) and ud.dt='$do_date'
union all
select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    2 retention_day
from dws_new_mid_day nm
join dws_uv_detail_day ud
on nm.mid_id=ud.mid_id
where nm.create_date='date_add('$do_date', -2)' and ud.dt='$do_date'

select
    nm.mid_id,
    nm.user_id ,
    nm.version_code ,
    nm.version_name ,
    nm.lang ,
    nm.source,
    nm.os,
    nm.area,
    nm.model,
    nm.brand,
    nm.sdk_version,
    nm.gmail,
    nm.height_width,
    nm.app_time,
    nm.network,
    nm.lng,
    nm.lat,
    nm.create_date,
    1 retention_day
from dws_new_mid_day nm
join dws_uv_detail_day ud
on nm.mid_id=ud.mid_id
where nm.create_date='date_add('$do_date', -1)' and ud.dt='$do_date'
"

$hive -e "$sql"

# 注释


: <<EOF
2019-09-02 的 1 日留存
    需要的数据:(2019-09-04 计算)
    2019-09-02 的新增和 2019-09-03 的日活
2019-09-02 的 2 日留存
    需要的数据:(2019-09-05 计算)
    2019-09-02 的新增和 2019-09-04 的日活
2019-09-02 的 3 日留存
    需要的数据:(2019-09-06 计算)
    2019-09-02 的新增和 2019-09-05 的日活

---

计算留存率应该计算哪些天的哪些留存率
2019-09-06 的计算任务(计算 2019-09-05 数据)
   2019-09-02 的 3 日留存 (2019-09-02 的新增和 2019-09-05 的日活)
       select
            nm.mid_id,
            nm.user_id ,
            nm.version_code ,
            nm.version_name ,
            nm.lang ,
            nm.source,
            nm.os,
            nm.area,
            nm.model,
            nm.brand,
            nm.sdk_version,
            nm.gmail,
            nm.height_width,
            nm.app_time,
            nm.network,
            nm.lng,
            nm.lat,
            nm.create_date,
            3 retention_day
       from dws_new_mid_day nm
       join dws_uv_detail_day ud
       on nm.mid_id=ud.mid_id
       where nm.create_date='2019-09-02' and ud.dt='2019-09-05'

   2019-09-03 的 2 日留存 (2019-09-03 的新增和 2019-09-05 的日活)
        select
            nm.mid_id,
            nm.user_id ,
            nm.version_code ,
            nm.version_name ,
            nm.lang ,
            nm.source,
            nm.os,
            nm.area,
            nm.model,
            nm.brand,
            nm.sdk_version,
            nm.gmail,
            nm.height_width,
            nm.app_time,
            nm.network,
            nm.lng,
            nm.lat,
            nm.create_date,
            2 retention_day
       from dws_new_mid_day nm
       join dws_uv_detail_day ud
       on nm.mid_id=ud.mid_id
       where nm.create_date='2019-09-03' and ud.dt='2019-09-05'


   2019-09-04 的 1 日留存 (2019-09-04 的新增和 2019-09-05 的日活)

       select
            nm.mid_id,
            nm.user_id ,
            nm.version_code ,
            nm.version_name ,
            nm.lang ,
            nm.source,
            nm.os,
            nm.area,
            nm.model,
            nm.brand,
            nm.sdk_version,
            nm.gmail,
            nm.height_width,
            nm.app_time,
            nm.network,
            nm.lng,
            nm.lat,
            nm.create_date,
            1 retention_day
       from dws_new_mid_day nm
       join dws_uv_detail_day ud
       on nm.mid_id=ud.mid_id
       where nm.create_date='2019-09-04' and ud.dt='2019-09-05'
EOF

