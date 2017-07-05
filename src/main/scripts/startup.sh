#!/usr/bin/env bash

# 现在柳州索引的情况：
# Http索引了一天中的0 ~ 0.2的数据大约120万
# FTP索引了29、28、27号的数据大约不到100万
# 聊天索引了29、28、27号的数据大约不到150万
# 其他的全量索引


# Http：由于Http的数据量比较大,一天的数据分多次执行,
# 第一个参数, 索引哪一天的数据
# 第二个参数,开始的范围
# 第三个参数,结束的范围
# 范围最小是0, 最大是1
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-29 0 0.2
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-29 0.2 0.4
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-29 0.4 0.6
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-29 0.6 0.8
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-29 0.8 1

#聊天的数据,数据量稍大，按天执行
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex 2017-06-29
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex 2017-06-28
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex 2017-06-27

# Ftp的数据，数据量稍大，按天执行
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex 2017-06-29
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex 2017-06-28
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex 2017-06-27

# 其他的数据，由于其他的数据都不大，下面的命令都是全量执行的,重复执行数据会重
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.BbsOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.EmailOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RealOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.SearchOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ServiceOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ShopOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.VidOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.WeiboOracleDataCreateSolrIndex

