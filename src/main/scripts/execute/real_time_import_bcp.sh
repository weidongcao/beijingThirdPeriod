#!/bin/bash

## 实时将Bcp文件ftp，im_chat,http类型的数据导入到Solr和HBase

# 跳转到程序的根目录
cd /opt/modules/BeiJingThirdPeriod/

# 执行程序
java -classpath BeiJingThirdPeriod.jar com.rainsoft.bcp.BcpFileImport

# 如果退出的话判断程序是否结束
while [[ true ]];
do
	# 查看进程是否存在,存在的话获取进程PID
	exit_status=`ps -ef | grep real_time_import_bcp.sh | grep -v grep | awk '{print $2}'`
	if [ ! ${exit_status} ];then
		echo "进程已结束，程序退出"
		break;
	else
		kill -9 ${exit_status}
	fi
done


