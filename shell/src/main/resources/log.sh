#!/bin/bash
# 日志群起脚本
for host in hadoop201 hadoop202 ; do
    echo ============ ${host} ============
    ssh ${host} "source /etc/profile ; java -jar /home/atguigu/dw-gmall/data-collector/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar 1>/dev/null 2>&1"
done


