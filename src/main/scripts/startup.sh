#!/usr/bin/env bash

# 参数说明：
# 第一个参数：Oracle的里的数据类型(有3个类型：ftp, http, imchat)，如果是FTP的数据就是ftp
# 第二个参数：开始时间
# 第三个参数：结束时间

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
#
#
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex ftp 2016-07-11 2016-07-11
#java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex ftp 2016-07-11 2016-07-01

java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex http 2016-06-04 2016-06-04
#java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex http 2016-06-04 2016-06-01

java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex imchat 2016-07-21 2016-07-21
#java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex imchat 2016-07-21 2016-07-01