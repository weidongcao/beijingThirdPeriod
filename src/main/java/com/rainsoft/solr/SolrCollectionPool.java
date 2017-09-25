package com.rainsoft.solr;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * Created by CaoWeiDong on 2017-07-31.
 */
public class SolrCollectionPool  implements Serializable {
    private static Logger log = LoggerFactory.getLogger(SolrCollectionPool.class);
    public static SolrCollectionPool instance = new SolrCollectionPool();
    private static Map<String, BlockingQueue<CloudSolrClient>> poolMap = new ConcurrentHashMap<>();
    public SolrCollectionPool() {

    }

    public synchronized BlockingQueue<CloudSolrClient> getCollectionPool(String zkHost, String collection, final int size) {
//        String zkHost = ConfigurationManager.getProperty("zkHost");
        if (poolMap.get(collection) == null) {
            log.info("solr:" + collection + " poolsize:" + size);
            System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
            System.setProperty("javax.xml.parsers.SAXParserFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
            BlockingQueue<CloudSolrClient> list = new LinkedBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                CloudSolrClient client = new CloudSolrClient.Builder().withZkHost(zkHost).build();
                client.setDefaultCollection(collection);
                client.setZkClientTimeout(1000 * 60);
                client.setZkConnectTimeout(1000 * 60);
                client.connect();
                list.add(client);
            }
            poolMap.put(collection, list);
        }
        return poolMap.get(collection);
    }

    public static SolrCollectionPool instance() {
        return SolrCollectionPool.instance;
    }
}
