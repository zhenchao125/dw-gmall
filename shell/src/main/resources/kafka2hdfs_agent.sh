#!/bin/bash

case $1 in
    start)
        echo ========= hadoop104 =========
        ssh hadoop104 "source /etc/profile ; nohup /opt/module/flume-1.7.0/bin/flume-ng agent -n a1 -c /opt/module/flume-1.7.0/conf -f /home/atguigu/dw-gmall/flume/kafka2hdfs.conf -Dflume.root.logger=info,console 1>/dev/null 2>&1&"

    ;;
    stop)
        echo ========= $host =========
        ssh hadoop104 "ps -ef | awk '/kafka2hdfs.conf/ && !/awk/{print \$2}'|xargs kill -9"
    ;;

    *)
        echo "kafka 数据 sink 到 hdfs 脚本使用方法: "
        echo "      start 启动迁移脚本"
        echo "      stop  停止迁移脚本"
    ;;
esac