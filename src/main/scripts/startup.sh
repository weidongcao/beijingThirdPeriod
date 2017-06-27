#!/usr/bin/env bash

java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.BbsOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.EmailOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RealOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.SearchOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ServiceOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ShopOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.VidOracleDataCreateSolrIndex
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.WeiboOracleDataCreateSolrIndex


java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex 2017-06-28
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex 2017-06-28 0 0.3
java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex 2017-06-28

