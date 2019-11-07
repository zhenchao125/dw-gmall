#!/bin/bash

db_name=gmall

export_data() {
/opt/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://hadoop102:3306/${db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password aaaaaa \
--table $1 \
--num-mappers 1 \
--export-dir /warehouse/$db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--input-null-string '\\N'    \
--input-null-non-string '\\N'
}

case $1 in
  "ads_uv_count")
     export_data "ads_uv_count" dt
;;
  "ads_user_action_convert_day")
     export_data "ads_user_action_convert_day" dt
;;
  "ads_gmv_sum_day")
     export_data "ads_gmv_sum_day" dt
;;
   "all")
     export_data "ads_uv_count" dt
     export_data "ads_user_action_convert_day" dt
     export_data "ads_gmv_sum_day" dt
;;
esac
