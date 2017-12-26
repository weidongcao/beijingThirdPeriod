package com.rainsoft.solr.demo;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateFormatUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * 测试通过Solr的Java API操作Collection
 * Created by CaoWeiDong on 2017-12-13.
 */
public class SolrCollectionTest {
    public static final String zkhost = ConfigurationManager.getProperty("zkHost");

    static SolrZkClient zkclient = new SolrZkClient(zkhost, 30000);

    static ZkConfigManager zkmanager = new ZkConfigManager(zkclient);

//    static SolrClient client = new CloudSolrClient.Builder()
//            .withZkHost(zkhost)
//            .build();
    public static void uploadConf(String confName, String confPath) throws IOException {
        zkmanager.uploadConfigDir(Paths.get(confPath), confName);
    }

    public static void delCollection(String collectionName) {
        CollectionAdminRequest.deleteCollection(collectionName);
    }

    /**
     *
     * @param collectionName Solr 集合名
     * @param confName  Zookeeper上的Solr配置文件所在目录
     * @param numShards 分片数
     * @param numReplicas 副本数
     * @throws IOException
     * @throws SolrServerException
     */
    public static void createCollection(String collectionName, String confName, int numShards, int numReplicas) throws IOException, SolrServerException {
        SolrClient client = new CloudSolrClient.Builder().withZkHost(zkhost).build();
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, confName, numShards, numReplicas);
        create.setMaxShardsPerNode(2);
        CollectionAdminResponse response = create.process(client);
        System.out.println(response);
        client.close();
    }


    public static void main(String[] args) throws IOException, SolrServerException, ParseException {
        createCollection("test1", "yisou", 3, 2);
    }
}
