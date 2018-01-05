#!/bin/sh

# 获取文件根路径
basepath=$(cd `dirname $0`; pwd)
if [ -e ${bashpath}/logging.sh ]; then
    source ${bashpath}/logging.sh
else 
    echo -e "\033[41;37m /opt/script/shell/log_function.sh is not exist. \033[0m"
    exit 1
fi

USER=`whoami`
if [ $USER == root ]; then
    log_info "execute by root"
else
    log_error "execute by ${USER}"
    echo -e "\033[41;37m you must execute this scritp by root. \033[0m"
    exit 1
fi

if [ -e /opt/script/shell/message ]; then
    echo 0 > /opt/script/shell/message
    logger "echo 0 > /opt/script/shell/message"
fi
