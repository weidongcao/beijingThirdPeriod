#!/usr/bin/env bash

# 脚本用法:
## 执行导入: nohup sh oracle_real_time_export.sh start &
## 结束导入：sh oracle_real_time_export.sh stop

# 跳转到程序的根目录
cd /opt/modules/BeiJingThirdPeriod/

javaCmd="java -classpath BeiJingThirdPeriod.jar "
javaPachage="com.rainsoft.run"
javaClass="RunOracleAllTableExport"

cmd="${javaCmd} ${javaPachage}.${javaClass}"
selfPid="$$"

## 打印日志
function logger(){
	echo "[`date "+%Y-%m-%d %H:%M:%S"`  INFO] ${1}"
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
function killApp() {
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
	killApp $0
	if [ $? -eq 0 ]; then
		killApp ${javaClass}
	fi
}

startStop=${1}

# 启动或者停止脚本
if [ -z "${startStop}" ]; then
	logger "您没有设置启动参数,用法:"
	logger "执行导入: nohup sh $0 start &"
	logger "结束导入：sh $0 stop"
elif [[ "start" == "${startStop}" ]]; then
	# 先检查程序是否已经启动
	checkAppStart

	# 执行程序
	logger "开始进行数据导入"
	logger "导入命令: ${cmd}"

	${cmd}
elif [[ "stop" == "${startStop}" ]]; then
	## 结束程序
	logger "开始结束数据导入进程"
	exitProgram
fi
