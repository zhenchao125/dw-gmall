#!/bin/bash
names=("hadoop 集群" "zookeeper 集群" "kafka 集群")
descriptors=("包含 hdfs yarn 历史服务器" "" "")
all_script=(hadoop.sh zk.sh kafka.sh)

case $1 in
    start)
        for (( i = 0; i < ${#names[@]}; ++i )); do
            echo "是否启动 ${names[i]}(${descriptors[i]}) y/n  [y]"
            read input
            if [[ $input == y || $input == Y ||$input == yes ||$input == YES || $input == "" ]]; then
                echo ============= 开始启动${names[$i]} =============
                ${all_script[$i]} start
            fi
        done
    ;;

    stop)
        for (( i = 0; i < ${#names[@]}; ++i )); do
            echo "是否停止 ${names[i]}(${descriptors[i]}) y/n  [y]"
            read input
            if [[ $input == y || $input == Y ||$input == yes ||$input == YES || $input == "" ]]; then
                echo ============= 开始停止${names[$i]} =============
                ${all_script[$i]} stop
            fi
        done
    ;;

    *)
        echo "使用方法: "
        echo "    start 参数启动所有集群"
        echo "    stop 参数启动所有集群"
esac



