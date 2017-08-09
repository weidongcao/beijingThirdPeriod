#!/usr/bin/env bash

for bcp in http bbs email search ftp weibo im_chat service_info
do
	bcppath=/opt/bcp/$bcp
	if [ -z "`ls ${bcppath}`" ];then
		echo "mv -f /opt/bcp/*-$bcp*.bcp ${bcppath}"
                mv -f /opt/bcp/*-$bcp*.bcp  ${bcppath}
	else
		echo "$bcppath is not empty."
	fi
done

for job_type in ftp im_chat http
do
    /opt/modules/spark/bin/spark-submit --master local[4] --executor-memory 32g --executor-cores 2 --num-executors 2 --class com.rainsoft.bcp.Main /opt/modules/BeiJingThirdPeriod/BeiJingThirdPeriod.jar ${job_type}
done


