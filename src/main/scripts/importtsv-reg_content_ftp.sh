#!/bin/bash
#importtsv-reg_content_http.sh

echo "开始时间 ------> "`date "+%Y-%m-%d %H:%M:%S"`

# Oracle的表名
tableName=reg_content_ftp

# 本地工作空间
localWorkSpace=/opt/modules/BeiJingThirdPeriod/oracle_workspace
# 本地数据文件池目录
localDataPool=${localWorkSpace}/pool
# 本地工作目录下的数据目录
localDataWork=${localWorkSpace}/work

# HDFS工作空间
hdfsWorkSpace=/tmp/oracle
# HDFS数据根目录
hdfsDataDir=${hdfsWorkSpace}/data
# HDFS上HFile根目录
hdfsHFileDir=${hdfsWorkSpace}/hfile
# HDFS资源路径
hdfsResource=hdfs://nn1.hadoop.com:8020

hbaseTableName=`tr '[a-z]' '[A-Z]' <<<"h_${tableName}"`


# HBase根目录
hbasePath=/opt/cloudera/parcels/CDH/lib/hbase/bin/hbase
# HBase-Server.jar路径
hbaseServerJar=/opt/cloudera/parcels/CDH/jars/hbase-server-1.0.0-cdh5.6.1.jar
# HBase表的字段名
hbaseColumns=HBASE_ROW_KEY,CONTENT_FTP:SESSIONID,CONTENT_FTP:SERVICE_CODE,CONTENT_FTP:CERTIFICATE_TYPE,CONTENT_FTP:CERTIFICATE_CODE,CONTENT_FTP:USER_NAME,CONTENT_FTP:PROTOCOL_TYPE,CONTENT_FTP:ACCOUNT,CONTENT_FTP:PASSWD,CONTENT_FTP:FILE_NAME,CONTENT_FTP:FILE_PATH,CONTENT_FTP:ACTION_TYPE,CONTENT_FTP:IS_COMPLETED,CONTENT_FTP:DEST_IP,CONTENT_FTP:DEST_PORT,CONTENT_FTP:SRC_IP,CONTENT_FTP:SRC_PORT,CONTENT_FTP:SRC_MAC,CONTENT_FTP:CAPTURE_TIME,CONTENT_FTP:ROOM_ID,CONTENT_FTP:CHECKIN_ID,CONTENT_FTP:MACHINE_ID,CONTENT_FTP:DATA_SOURCE,CONTENT_FTP:FILE_SIZE,CONTENT_FTP:FILE_URL,CONTENT_FTP:MANUFACTURER_CODE,CONTENT_FTP:ZIPNAME,CONTENT_FTP:BCPNAME,CONTENT_FTP:ROWNUMBER,CONTENT_FTP:IMPORT_TIME,CONTENT_FTP:SERVICE_CODE_IN,CONTENT_FTP:TERMINAL_LONGITUDE,CONTENT_FTP:TERMINAL_LATITUDE

function logger() {
  echo "[`date "+%Y-%m-%d %H:%M:%S"` INFO] ${1}"
}

logger "tableName = ${hbaseTableName}"
# 判断本地工作目录是否存在，如果不存在的话创建
if [ ! -d "${localDataWork}/${tableName}" ]; then
  mkdir -p ${localDataWork}/${tableName}
fi

# HDFS上创建Oracle表名目录
hdfs dfs -mkdir -p ${hdfsDataDir}/${tableName}
# HDFS上创建HFile的根目录
hdfs dfs -mkdir -p ${hdfsHFileDir}

# 将数据迁移到HBase
while [[ 1 = 1 ]]; do
  #当数据池内的数据文件有100个以上的时候才执行
  logger "统计数据池内的文件个数,命令: find ${localDataPool}/${tableName} -name "${tableName}_data_*.tsv" | wc -l"
  dataFileCount=`find ${localDataPool}/${tableName} -name "${tableName}_data_*.tsv" | wc -l`
  logger "数据池内的文件个数: ${dataFileCount}"
  
  if [[ ${dataFileCount} -gt 10 ]]; then
        ## 将tsv文件清空
      find ${localDataWork}/${tableName} -name "${tableName}_data_*.tsv" | xargs -i rm -f {}
      
      # 将数据文件从文件池目录转移到工作目录
      find ${localDataPool}/${tableName} -name "${tableName}_data_*.tsv" | tail -n 10 | xargs -i mv {} ${localDataWork}/${tableName}
      
      ## 将HDFS上的TSV目录清空,注意，不能删除
      hdfs dfs -rm -f ${hdfsDataDir}/${tableName}/*
      ## 将tsv文件上传到HDFS
      logger "开始上传tsv文件到HDFS......"
      hdfs dfs -put -p ${localDataWork}/${tableName}/${tableName}_data_*.tsv ${hdfsDataDir}/${tableName}

      ## 删除HFile目录，生存HFile文件必须保证此目录不存在
      hdfs dfs -rm -r ${hdfsHFileDir}/${tableName}

      ## 将HDFS上的tsv文件转换为HFile文件
      logger "开始将HDFS上的tsv文件转换为HFile文件......"
      HADOOP_CLASSPATH=`${hbasePath} classpath` \
      yarn jar ${hbaseServerJar} \
      importtsv \
      -Dimporttsv.columns=${hbaseColumns} \
      -Dimporttsv.bulk.output=${hdfsResource}${hdfsHFileDir}/${tableName} \
      ${hbaseTableName} \
      ${hdfsResource}${hdfsDataDir}/${tableName}
      logger "生成HFile文件完毕"

      ###修改DFS文件的权限
      hadoop dfs -chmod -R 777 ${hdfsWorkSpace}

      ## 将转换后的HFile文件加载到HBase
      logger "开始将转换后的HFile文件加载到HBase......"
      HADOOP_CLASSPATH=`${hbasePath} classpath` yarn jar \
      ${hbaseServerJar} completebulkload ${hdfsResource}${hdfsHFileDir}/${tableName} ${hbaseTableName}
      logger "HFile文件加载HBase完毕"

      ## 删除HFile目录，生存HFile文件必须保证此目录不存在
      hdfs dfs -rm -r ${hdfsHFileDir}/${tableName}

      ## 将HDFS上的TSV目录清空,注意，不能删除
      hdfs dfs -rm -f ${hdfsDataDir}/${tableName}/*

      ## 将tsv文件清空
      find ${localDataWork}/${tableName} -name "${tableName}_data_*.tsv" | xargs -i rm -f {}
      logger "<--------------------------------------- 处理完毕 --------------------------------------->"
      sleep 20s
  else
    ## 数据文件池内的文件太少，继续等待
    logger "数据文件池内的文件太少，继续等待"
    sleep 1m
  fi
done
