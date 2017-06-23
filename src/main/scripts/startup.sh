#!/usr/bin/env bash

# 参数说明：
# 第一个参数：开始时间
# 第二个参数：结束时间
# 第三个参数：创建索引的方式: 一次性索引完一天所有的数据(once);分批次索引数据(several)
# 第四个参数：如果分批次索引数据的话一次索引多少条数据,比如说一天有10亿条数据,可以分10次一次索引1亿条数据

# 时间说明：
# 开始时间要大于等于结束时间
# 如果跑一天的数据开始时间等于结束时间
#
# 如果跑多天的数据，开始时间大于结束时间，
# 比如说开始时间是2017-06-16，结束时间2017-06-01
# 程序会先跑2017-06-16的数据，
# 然后跑2017-06-15  。。。
# 最后跑2017-06-01
#
# 2016-07-11 2016-07-11 8000 several
#

# 一次性索引完一天所有的数据:
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex 2017-06-21 2017-06-21 once

# 分批次索引一天的数据,一次索引一万条数据：
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex 2017-06-21 2017-06-21 several 10000