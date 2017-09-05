#!/bin/bash
## 将BCp文件的数据导入到Solr和HBase


# BCP文件转TSV
bcp_transform_tsv_cmd_template="java -classpath BeiJingThirdPeriod.jar com.rainsoft.bcp.TransformBcp2Tsv"

# TSV文件导入HBase
bcp_import_hbase_cmd_template="spark-submit --master local[4] --driver-memory 16g --executor-memory 4g --class com.rainsoft.bcp.Main /opt/modules/BeiJingThirdPeriod/BeiJingThirdPeriod.jar"
# TSV 导入Solr
bcp_import_solr_cmd_template="java -classpath BeiJingThirdPeriod.jar com.rainsoft.bcp.BcpToSolr"

# 打印日志
function logger() {
    # 写入日志文件
    echo "[`date "+%Y-%m-%d %H:%M:%S"`  INFO] ${1}" >> logs/bcp_to_hbase_solr-`date "+%Y%m%d"`.log
    #echo "[`date "+%Y-%m-%d %H:%M:%S"`  INFO] ${1}"
}

# 第一个参数：命令
# 第二个参数：命令的参数
function run_cmd(){
    cmdStartTime=`date +%s`
    logger "要执行的命令：${1} ${2}"
    ${1} ${2} >> logs/bcp_to_hbase_solr-`date "+%Y%m%d"`.log
    #echo "${1} ${2}"
    cmdRunResultStatus=$?
    cmdEndTime=`date +%s`
    if [[ ${cmdRunResultStatus} -eq 0 ]]; then
        logger "命令执行完成,耗时: $(((${cmdEndTime} - ${cmdStartTime})/60)) 分钟, $(((${cmdEndTime} - ${cmdStartTime})%60)) 秒"
    else
        logger "命令执行失败,耗时: $(((${cmdEndTime} - ${cmdStartTime})/60)) 分钟, $(((${cmdEndTime} - ${cmdStartTime})%60)) 秒"
		logger "休眠一个小时..."
        # 程序休眠一个小时
        sleep 1h
    fi
    return ${cmdRunResultStatus}
}

# 执行数据导入(导入HBase和Solr)
# 参数说明
# 就一个参数:task类型(ftp, im_chat, http)
function run_import() {
    # BCP数据导入到HBase
    run_cmd "${bcp_import_hbase_cmd_template}" "${1}"

    # BCP数据索引到Solr
    run_cmd "${bcp_import_solr_cmd_template}" "${1}"
    # 判断BCP数据索引到Solr是否成功完成,如果成功完成的话删除本地TSV文件
    if [[ $? -eq 0 ]]; then
      #删除TSV文件
      rm -f /opt/modules/BeiJingThirdPeriod/oracle_workspace/work/bcp-${1}/*
      logger "删除TSV文件完成: ${1}"
      # 删除BCP文件
      rm -f /opt/bcp/${1}/*
      logger "删除BCP文件完成: ${1}"
    fi
}

rm -f /opt/bcp/ftp/*
rm -f /opt/bcp/im_chat/*
rm -f /opt/bcp/http/*

while [[ 1 = 1 ]];
do
    # job start time
    jobStartTime=`date +%s`
    for task in im_chat ftp http
    do
        bcppath=/opt/bcp/${task}

        # 判断TSV文件工作目录下有没有文件，如果有文件，先处理这些文件
        tsvFileCount=`ls /opt/modules/BeiJingThirdPeriod/oracle_workspace/work/bcp-${task} | wc -l`
        if [[ ${tsvFileCount} -gt 0 ]]; then
            # TSV数据导入到HBase和Solr
            logger "有未处理完成的TSV数据文件,类型为: ${task}.先处理此TSV文件..."
            run_import "${task}"
        fi

        # 再次判断TSV文件工作目录下有没有文件，如果没有文件，再处理BCP文件
        tsvFileCount=`ls /opt/modules/BeiJingThirdPeriod/oracle_workspace/work/bcp-${task} | wc -l`

        # 如果TSV工作目录没有tsv文件且BCP文件池内同一类型的文件大于40个再进行处理，否则等待
        if [[ ${tsvFileCount} -eq 0 ]]; then
            # 统计数据池内有多少个此类型的BCP文件
            bcpFileCount=`find /rsdata/out -name "*-${task}*.bcp" | wc -l`
            if [[ ${bcpFileCount} -gt 40 ]]; then
				logger "开始处理${task}类型的BCP文件..."
                # task start time
                taskStartTime=`date +%s`

                logger "find /rsdata/out -name "*-${task}*.bcp"  | tail -n 200 |xargs -i mv {} ${bcppath}"
                find /rsdata/out -name "*-${task}*.bcp"  | tail -n 200 |xargs -i mv {} ${bcppath}
                logger "已将 ${task} 数据文件从数据文件池移动到 ${bcppath}"

                # BCP文件转TSV文件
                run_cmd "${bcp_transform_tsv_cmd_template}" "${task}"

                # TSV数据导入到HBase和Solr
                run_import "${task}"

                # task end time
                taskEndTime=`date +%s`
                logger "<-------- ${task} 任务处理完成, 耗时: $(((${taskEndTime} - ${taskStartTime})/60)) 分钟, $(((${taskEndTime} - ${taskStartTime})%60)) 秒 -------->"
            else
                ## 数据文件池内的文件太少，继续等待
                logger "数据文件池内的 ${task} 文件数量为:${bcpFileCount},少于40个,继续等待..."
            fi
        else
            # TSV文件没有处理成功
            logger "有未处理的TSV文件,等待TSV文件处理完成再处理BCP文件"
        fi
        sleep 1s
    done
    # job end time
    jobEndTime=`date +%s`
    logger ""

    logger "###########################################################"
    logger "############## 一次job处理完成，$(((${jobEndTime} - ${jobStartTime})/60)) 分钟, $(((${jobEndTime} - ${jobStartTime})%60)) 秒 ##############"
    logger "###########################################################"
    sleep 20s
done
