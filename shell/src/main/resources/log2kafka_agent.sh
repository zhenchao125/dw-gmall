#!/bin/bash

case $1 in
    start)
        for host in hadoop201 ; do
            echo ========= $host =========
            ssh $host "source /etc/profile ; nohup /opt/module/flume-1.7.0/bin/flume-ng agent -n  a1 -c /opt/module/flume-1.7.0/conf -f /home/atguigu/dw-gmall/flume/log2kafka.conf >/dev/null 2>&1 &"
        done

    ;;
    stop)
        for host in hadoop201 ; do
            echo ========= $host =========
            ssh $host "ps -ef | awk '/log2kafka.conf/ && !/awk/{print \$2}'|xargs kill -9"
        done
    ;;

    *)
        echo 你启动的姿势不对, 换个姿势再来
    ;;
esac