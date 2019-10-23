#!/usr/bin/env bash
hadoop_home=/opt/module/hadoop-2.7.2/
case $1 in
    start)
        echo ========== 在 hadoop102 上启动 HDFS ==========
        $hadoop_home/sbin/start-dfs.sh
        echo ========== 在 hadoop103 上启动 YARN ==========
        ssh hadoop103 "$hadoop_home/sbin/start-yarn.sh"
        echo ========== 在 hadoo102 上启动 历史服务器 ==========
        ssh hadoop102 "$hadoop_home/sbin/mr-jobhistory-daemon.sh start historyserver"
    ;;
    stop)
        echo ========== 关闭 HDFS ==========
        $hadoop_home/sbin/stop-dfs.sh
        echo ========== 关闭 YARN ==========
        ssh hadoop103 "$hadoop_home/sbin/stop-yarn.sh"
        echo ========== 关闭 历史服务器 ==========
        ssh hadoop102 "$hadoop_home/sbin/mr-jobhistory-daemon.sh stop historyserver"
    ;;

    *)
        echo 你使用脚本的姿势不对, 启动 hadoop 集群请添加参数 start, 关闭 hadoop 集群请添加参数 stop
    ;;

 esac


