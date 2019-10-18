#!/bin/bash

# 集体设置系统时间
for host in hadoop201 hadoop202 hadoop203 ; do
    echo ========= $host =========
    ssh $host "echo ========= $host 修改前的时间: `date` ========= ; date -s $1 ; echo ========= $host 修改后的时间: `date` ========="
done
