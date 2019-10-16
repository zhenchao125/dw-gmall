#!/bin/bash
kafka_home=/opt/module/kafka_2.11-0.11.0.3/

case $1 in
    start)
        for host in hadoop201 hadoop202 hadoop203 ; do
            echo ========== $host =========
            ssh $host "source /etc/profile ; $kafka_home/bin/kafka-server-start.sh -daemon $kafka_home/config/server.properties"

        done
    ;;
    stop)
        for host in hadoop201 hadoop202 hadoop203 ; do
            echo ========== $host =========
            ssh $host "source /etc/profile ; $kafka_home/bin/kafka-server-stop.sh"

        done
    ;;

 esac




