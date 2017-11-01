#!/bin/bash

## 将Oracle的ftp,im_chat,http类型数据通过Spark导出到Solr，HBase

# 跳转到程序的根目录
cd /opt/modules/BeiJingThirdPeriod/

# 执行程序
# 参数说明
# 第一个参数，导入结束时间
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RunOracleAllTableExport "2017-10-20 00:00:00"

# 如果退出的话判断程序是否结束
while [[ true ]];
do
	# 查看进程是否存在,存在的话获取进程PID
	exit_status=`ps -ef | grep oracle_history_export.sh | grep -v grep | awk '{print $2}'`
	if [ ! ${exit_status} ];then
		echo "进程已结束，程序退出"
		break;
	else
		kill -9 ${exit_status}
	fi
done


