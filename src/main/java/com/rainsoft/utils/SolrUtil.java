package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Administrator on 2017-04-06.
 */
public class SolrUtil {
    private static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);

    //Solr的Zookeeper地址
//    private static String zkHost = "dn1.hadoop.com,dn2.hadoop.com,dn3.hadoop.com,nn1.hadoop.com,nn2.hadoop.com";
    private static String zkHost = ConfigurationManager.getProperty("zkHost");
    //Solr客户端
    private static CloudSolrClient client;

    /**
     * 获取Solr客户端连接
     * @param collection Solr的核心（集合）
     * @return Solr客户端连接
     */
    public static CloudSolrClient getSolrClient(String collection) {
        System.out.println(zkHost);
        if (client == null) {
            client = new CloudSolrClient.Builder().withZkHost(zkHost).build();
            logger.info("Solr 客户端初始化成功");
        }
        client.setDefaultCollection(collection);

        return client;
    }

    public static boolean delSolrByCondition(String condition) {
        UpdateRequest commit = new UpdateRequest();

        boolean commitStatus = false;
        try {
            commit.deleteByQuery(condition);
            commit.setCommitWithin(10000);
            commit.process(client);
            commitStatus = true;
        } catch (SolrServerException e) {
            logger.error("Solr 删除数据失败： {}", e);
        } catch (IOException e) {
            logger.error("Solr 删除数据失败： {}", e);
            e.printStackTrace();
        }
        return commitStatus;
    }

}
