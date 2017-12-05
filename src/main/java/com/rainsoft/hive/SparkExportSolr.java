package com.rainsoft.hive;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark将通过Hive将HBase的数据索引到Solr
 * Created by CaoWeiDong on 2017-07-25.
 */
public class SparkExportSolr {
    private static final Logger logger = LoggerFactory.getLogger(SparkExportSolr.class);

    private static final String SOLR_URL = ConfigurationManager.getProperty("solr.url");

    //创建Solr客户端
    protected static SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();
//    protected static CloudSolrClient client = SolrUtil.getClusterClient("yisou");

    public static void main(String[] args) {

    }
}
