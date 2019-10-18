#!/bin/bash

case $1 in
    start | stop | status)
        for host in hadoop102 hadoop103 hadoop104 ; do
            echo ========= $host =========
            ssh $host "source /etc/profile ; /opt/module/zookeeper-3.4.10/bin/zkServer.sh $1"
        done

    ;;

    *)
        echo 你使用脚本的姿势不对, 启动 zookeeper 集群请添加参数 start, 关闭 zookeeper 集群请添加参数 stop, 查看 zookeeper 集群状态请添加参数 status
    ;;
esac