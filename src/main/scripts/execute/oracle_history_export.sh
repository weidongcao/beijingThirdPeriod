#!/usr/bin/env bash

# 脚本用法:
## 执行导入: nohup sh oracle_history_export.sh start "${结束时间,如:2017-12-01 00:00:00}" &
## 例子: nohup sh oracle_history_export.sh start "2017-12-01 00:00:00" &
## 结束导入：sh oracle_history_export.sh stop

# 跳转到程序的根目录
cd /opt/modules/BeiJingThirdPeriod/

javaCmd="java -classpath BeiJingThirdPeriod.jar "
javaPachage="com.rainsoft.solr"
javaClass="RunOracleAllTableExport"

cmd="${javaCmd} ${javaPachage}.${javaClass}"
selfPid="$$"

## 打印日志
function logger(){
	echo "[`date "+%Y-%m-%d %H:%M:%S"`  INFO] ${1}"
}

## 脚本用法
function usage(){
	logger "执行导入的命令: nohup sh $0 start \"\${结束时间}\" &"
	logger "例如: nohup sh $0 start \"2017-12-01 00:00:00\" &"
	logger "结束导入的命令: sh $0 stop"
}

## 启动程序之前先检查程序是否已经启动
function checkAppStart(){
	logger "检查程序是否已经启动"
	pid=`ps -ef | grep ${0} | grep -v grep | grep -v ${selfPid} | awk '{print $2}'`
	if [ -n "${pid}" ]; then
		logger "程序已经启动pid为: ${pid}, 如需要重启, 请先停止程序"
		exit 1
	fi
}

## 杀掉进程
function killProcess() {
	cmdRunResultStatus=-1
	# 如果退出的话判断程序是否结束
	while [ ${cmdRunResultStatus} -ne 0 ];
	do
		# 查看进程是否存在,存在的话获取进程PID
		pid=`ps -ef | grep ${1} | grep -v grep | grep -v ${selfPid} | awk '{print $2}'`
		if [ -z "${pid}" ]; then
			logger "进程没有启动: ${1}"
			if [ ${cmdRunResultStatus} -eq -1 ]; then
				cmdRunResultStatus=0
			fi
			break;
		else
			sleep 1s
			kill -9 ${pid}
			cmdRunResultStatus=$?
			if [ ${cmdRunResultStatus} -eq 0 ]; then
				logger "进程结束: ${pid}    ${1}"
				logger "等待进程资源释放..."
				while [[ true ]];
				do
					pid=`jps | grep ${1} | grep -v grep | grep -v $$ | awk '{print $1}'`
					if [ -z "${pid}"]; then
					
						logger "进程资源释放完成."
						break
					else
						sleep 1s
					fi
				done
			fi		
		fi
	done


	return ${cmdRunResultStatus}
}

## 结束程序
function exitProgram() {
	killProcess $0
	if [ $? -eq 0 ]; then
		killProcess ${javaClass}
	fi
}

startStop=${1}
shift


# 启动或者停止脚本
## 参数为空
if [ -z "${startStop}" ]; then
	logger "您没有设置启动参数,用法:"
	usage
elif [[ "start" == "${startStop}" ]]; then
	# 先检查程序是否已经启动
	checkAppStart

	if [ -z "${1}" ]; then
		logger "进行数据导入时您没有设置结束日期,请设置结束日期参数"
		logger "脚本用法:"
		usage
		exit 1
	else
		endTime=$1
	fi
	# 执行程序
	logger "开始进行数据导入"
	logger "导入命令: ${cmd}"
	${cmd} "${endTime}"
elif [[ "stop" == "${startStop}" ]]; then
	# 结束程序
	logger "开始结束数据导入"
	exitProgram
else
	logger "您输入的参数不正确"
	usage
fi
