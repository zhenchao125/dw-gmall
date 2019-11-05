#!/bin/bash

dates=(2019-12-03 2019-12-04 2019-12-05 2019-12-10 2019-12-11 2019-12-12 2019-12-17 2019-12-18 2019-12-19)
for date in ${dates[@]} ; do
    # 更改日期
    /home/atguigu/bin/change_date.sh ${date}
    sleep 10
    # 生成数据
    /home/atguigu/dw-gmall/data-collector/log.sh
    sleep 30
    #  ods
    /home/atguigu/dw-gmall/hive/ods_log.sh ${date}
    #  dwd
    /home/atguigu/dw-gmall/hive/dwd_start_log.sh ${date}
    /home/atguigu/dw-gmall/hive/dwd_base_log ${date}
    /home/atguigu/dw-gmall/hive/dwd_event_log.sh ${date}
    # dws
    /home/atguigu/dw-gmall/hive/dws_uv_log.sh ${date}
    /home/atguigu/dw-gmall/hive/new_mid_day.sh ${date}
    /home/atguigu/dw-gmall/hive/dws_user_retention_day.sh ${date}
    #aws
done

