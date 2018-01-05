#!/bin/bash
#log function

# 获取文件根路径
basepath=$(cd `dirname $0`; pwd)

# 判断日志文件夹是否存在,如果不存在的话创建
logpath=${basepath}/logs
if [ ! -d ${logpath} ]; then
    mkdir ${logpath}
fi

## log_info函数打印正确的输出到日志文件
function log_info () {
    DATE=`date "+%Y-%m-%d %H:%M:%S"` ####显示打印日志的时间
    USER=$(whoami) ####那个用户在操作
    echo "${DATE} ${USER} execute $0 [INFO] $@" >>${logpath}/log.log ###### ($0脚本本身，$@将参数作为整体传输调用)
}

## log_error打印shell脚本中错误的输出到日志文件
function log_error () {
    DATE=`date "+%Y-%m-%d %H:%M:%S"`
    USER=$(whoami)
    echo "\${DATE} \${USER} execute \$0 [INFO] \$@" >>${logpath}/error.log
}

## logger函数 通过if判断执行命令的操作是否正确，并打印出相应的操作输出
function logger (){
    if [ $? -eq 0 ]; then
        log_info "$@ sucessed!"
        echo -e "\033[32m $@ sucessed. \033[0m" 
    else
        log_error "$@ failed!"
        echo -e "\033[41;37m $@ failed. \033[0m"
        exit
    fi
}
