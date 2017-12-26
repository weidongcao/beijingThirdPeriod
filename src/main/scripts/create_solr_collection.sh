#!/bin/sh

# 脚本使用说明
# 初次使用需要根据集群情况修改以下参数
# host_name: 可以是Solr集群中的任意一台服务器的主机名或者IP地址
# solr_port: 此服务器Solr的端口号
# numShards: Solr集群的分片数量，与Solr集群的节点数一致
# replicationFactor: 副本数，数据量不大可以指定副本数为1，数据量大的话指定为2
# maxShardsPerNode: 单个节点最大分片数，与副本数一致

## 创建Collection的规则
# 脚本的功能为Solr每个月10天创建一个Collection
# Collection的命令格式为"yisou年月(天除了10)"
# 例如
# 2017年12月03日在2017年12月的第1个10天，此Collection命名为yisou20171201
# 2017年12月13日在2017年12月的第2个10天，此Collection命名为yisou20171202
# 2017年12月23日在2017年12月的第3个10天，此Collection命名为yisou20171203
# 2017年12月30日31日，就两天，也把它算到第3个10天，命令为yisou20171203

# 执行定时任务
# 定时执行此任务，每个月的9号，19号，28号的凌晨3点执行一次
# 每个月的9号为11号-20号创建Collection
# 每个月的19号为21号-30(具体看一个月什么时候结束，如果28号结束就到28号，
# 如果30号结束就到30号，31号就31号)号创建Collection
# 每个月的28号为下一个月的1号到10号创建Collection


# Solr所在主机名
host_name="http://cm02.spark.com"
# Solr所在主机名对应的端口号
solr_port=8080
# 分片数
numShards=3
# 副本数
replicationFactor=1
# 一个节点最多多少个分片
maxShardsPerNode=1

collection="yisou"
# 测试用，用来指定日期
curDate=`date -d "${1}" +%Y-%m-%d`
# curDate=`date +%Y-%m-%d`
# 年
curYear=`date -d "${curDate}" +%Y`

# 月
curMonth=`date -d "${curDate}" +%m`
# 去掉左边的0
if [[ "${curMonth}" != "10" ]]; then 
	curMonth=${curMonth#*0}
fi
echo "curMonth = ${curMonth}"

# 日
curDay=`date -d "${curDate}" +%d`
# 去掉左边的0
curDay=${curDay#*0}

# 按月划分Collection的下标
index=-1

# 这个月提前创建下个月的Collection，在每月的28号提前创建，
# 其他的都是提前半天，但是下个月的得在28号之前，因为2月可能没有29号
if [[ ${curDay} -ge 28 ]]; then
	# 日重置为1
	let curDay=1
	
	# 月份如果小于12月，月份加1
	if [[ ${curMonth} -lt 12 ]]; then
		let curMonth=`expr ${curMonth} + 1`
	else
		# 否则的话年份加1，月份重置为1
		let curYear=`expr ${curYear} + 1`
		let curMonth='01'
	fi
else
	# 创建当月的Collection都是提前半天创建，所以日加1
	let curDay=`expr ${curDay} + 1`
fi

# 如果月份是个位数，则在前面加0
if [[ ${#curMonth} -eq 1 ]]; then
	curMonth="0${curMonth}"
fi

# 10天创建一个一个Collection，日除了10作为此Collection的识别号
((index=${curDay}/10))

# 如果日是30号，31号的话归到上一个一个Collection，不再单独创建
if [[ ${index} -ge 3 ]]; then
	index=2
fi

# Collection全名
full_collection="${collection}${curYear}${curMonth}0${index}"
echo "要创建的Collection的名字为: ${full_collection}"

# 要执行的URL
create_url="${host_name}:${solr_port}/solr/admin/collections?action=CREATE&name=${full_collection}&numShards=${numShards}&replicationFactor=${replicationFactor}&maxShardsPerNode=${maxShardsPerNode}&collection.configName=${collection}"

echo "要执行的命令: ${create_url}"

# 执行命令
#result=`curl "${create_url}"`
echo "执行命令的返回结果: \r\n${result}"
