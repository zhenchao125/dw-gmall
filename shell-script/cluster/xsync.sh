#!/bin/bash
# 集群分发脚本

# 1. 获取参数个数, 如果没有参数则直接退出
if [[ $# == 0 ]]; then
    echo '请输入要分发的文件或者路径'
    exit
fi

# 2. 获取要分发的文件或者目录的名字
p1=$1
fileName=`basename ${p1}`
echo ${fileName}

## 3. 获取上级目录的绝对路径
pdir=`cd -P $(dirname ${p1}); pwd`
echo pdir=${pdir}

## 4. 获取当前用户名
user=`whoami`

## 5. 分发脚本
for host in hadoop201 hadoop202 hadoop203 ; do
    echo ========== $host =========
    rsync -rvl $pdir/$fileName $user@$host:$pdir
done
