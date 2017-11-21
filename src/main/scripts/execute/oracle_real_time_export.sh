#!/bin/bash

## 实时将Oracle内容表在表ftp，im_chat,http类型的数据导入到Solr和HBase

# 跳转到程序的根目录
cd /opt/modules/BeiJingThirdPeriod/

javaCmd="java -classpath BeiJingThirdPeriod.jar "
javaPachage="com.rainsoft.solr"
javaClass="RunOracleAllTableExport"

cmd="${javaCmd} ${javaPachage}.${javaClass}"

## 杀掉进程
function killProcess() {
	cmdRunResultStatus=""
	# 如果退出的话判断程序是否结束
	while [ ${cmdRunResultStatus} -ne 0 ];
	do
		# 查看进程是否存在,存在的话获取进程PID
		exit_status=`ps -ef | grep ${1} | grep -v grep | awk '{print $2}'`
		if [ -z ${exit_status} ];then
			echo "进程已结束，程序退出"
			break;
		else
			kill -9 ${exit_status}
			cmdRunResultStatus=$?
		fi
	done

	return ${cmdRunResultStatus}
}

## 结束程序
function exitProgram() {
	exitStatus=$(killProcess $0)
	if [ ${exitStatus} -eq 0 ]; then
		killProcess ${javaClass}
	fi
}

startStop=${1}
if [ -z "${startStop}"]; then
	startStop="start"
fi

# 启动或者停止脚本,如果没有参数的话默认开始执行导入
if [[ "start" == "${startStop}" ]]; then
	# 执行程序
	# 跑FTP、IM_CHAT、HTTP三个大表
	# java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RunOracleBigTableExport

	# 跑所有的内容表包括:ftp、im_chat,http, bbs,email,search,weibo,real,vid
	`${cmd}`
elif [[ "stop" == "${startStop}" ]]; then
	exitProgram
fi
