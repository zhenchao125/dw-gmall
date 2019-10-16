#!/bin/bash

case $1 in
    start | stop | status)
        for host in hadoop201 hadoop202 hadoop203 ; do
            echo ========= $host =========
            ssh $host "source /etc/profile ; /opt/module/zookeeper-3.4.10/bin/zkServer.sh $1"
        done

    ;;

    *)
        echo 你启动的姿势不对, 换个姿势再来
    ;;
esac