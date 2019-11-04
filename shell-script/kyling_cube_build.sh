#! /bin/bash
cube_name=cub1
if [[ -n $1 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

#获取00:00时间戳
start_date_unix=`date -d "$do_date 08:00:00" +%s`
start_date=$(($start_date_unix*1000))

#获取24:00的时间戳
stop_date=$(($start_date+86400000))

curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"startTime":'${start_date}', "endTime":'${stop_date}', "buildType":"BUILD"}' http://hadoop102:7070/kylin/api/cubes/$cube_name/build
