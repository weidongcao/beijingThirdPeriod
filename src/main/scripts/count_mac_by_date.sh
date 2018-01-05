#!/bin/bash

# 统计通过Hive的映射表统计HBase里scan_ending_trace_detail表的Mac地址出现的次数
# 执行此脚本的时候需要先创建Hive外部表:isec_ending_info_count
# 这个表映射HBase的isec_ending_info表的两个字段，一个是rowkey
# 一个是需要统计的字段:cnt_mac

cur_date=`date -d '2 days ago' "+%Y-%m-%d"`
if [[ $1 != "" ]]; then
    cur_date=`date -d "$1" "+%Y-%m-%d"`
fi
echo "要统计的日期: ${cur_date}"
start_date=`date -d "${cur_date}" "+%Y-%m-%d %H:%M%S"`
echo "要统计的开始时间: ${start_date}"
end_date=`date -d "${cur_date} +1 day" "+%Y-%m-%d %H:%M:%S"`
echo "要统计的结束时间: ${end_date}"

sql="""insert into table isec_ending_info_count \
select regexp_replace(ending_mac, '-',''), count(ending_mac) \
from scan_ending_trace_detail \
where capture_time > '{start_date}' and capture_time < '{end_date}' \
group by ending_mac;"""
echo "要执行的命令: ${sql}"

`hive <<EOF
${sql_template}
EOF`
