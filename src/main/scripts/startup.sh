#!/usr/bin/env bash

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

# 根据捕获时间单独执行一个表一天的数据：
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.BbsOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.EmailOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.SearchOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ShopOracleDataCreateSolrIndex 2017-01-01
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.WeiboOracleDataCreateSolrIndex 2017-01-01

# 下面的表没有捕获时间，执行索引全部的数据
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RealOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.VidOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ServiceOracleDataCreateSolrIndex

# 指定执行的命令
nohup python startup.py &

