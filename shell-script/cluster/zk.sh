#!/bin/bash

case $1 in
    start | stop | status)
        for host in hadoop102 hadoop103 hadoop104 ; do
            echo ========= $host =========
            ssh $host "source /etc/profile ; /opt/module/zookeeper-3.4.10/bin/zkServer.sh $1"
        done
    ;;

    *)
        echo "你使用脚本的姿势不对"
        echo "  zk.sh start  启动 zookeeper 集群"
        echo "  zk.sh stop   停止 zookeeper 集群"
        echo "  zk.sh status 查看 zookeeper 集群转态"
    ;;
esac