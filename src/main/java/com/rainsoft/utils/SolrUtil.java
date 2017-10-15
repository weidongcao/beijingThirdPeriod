package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 *
 * Created by Administrator on 2017-04-06.
 */
public class SolrUtil {
    private static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);
    private static SolrClient client;

    private static final DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 获取Solr的客户端连接
     * @return
     */
    public static SolrClient getClusterSolrClient() {
        if (client == null) {
            String zkHost = ConfigurationManager.getProperty("zkHost");
            CloudSolrClient clusterClient = new CloudSolrClient.Builder().withZkHost(zkHost).build();
            clusterClient.setZkClientTimeout(30000);
            clusterClient.setZkConnectTimeout(50000);
            clusterClient.setDefaultCollection(ConfigurationManager.getProperty("solr.collection"));
            client = clusterClient;
        }

        return client;
    }
    /**
     * 提交到Solr
     *
     * @param list
     * @param client 是否提交到集群版Solr
     * @return
     */
    public static boolean submit(List<SolrInputDocument> list, SolrClient client) {
        /*
         * 异常捕获
         * 如果失败尝试3次
         */
        int tryCount = 0;
        boolean flat = false;
        while (tryCount < 3) {
            try {
                if (!list.isEmpty()) {
                    client.add(list, 1000);
                }
                flat = true;
                //关闭Solr连接
//                client.close();
                //如果索引成功,跳出循环
                break;
            } catch (Exception e) {
                e.printStackTrace();
                tryCount++;
                flat = false;
            }
        }
        return flat;
    }

    public static boolean delSolrByCondition(String condition, SolrClient client) {

        UpdateRequest commit = new UpdateRequest();

        boolean commitStatus = false;
        try {
            commit.deleteByQuery(condition);
            commit.setCommitWithin(10000);
            commit.process(client);
            commitStatus = true;
//            client.close();
        } catch (SolrServerException e) {
            logger.error("Solr 删除数据失败： {}", e);
        } catch (IOException e) {
            logger.error("Solr 删除数据失败： {}", e);
            e.printStackTrace();
        }
        return commitStatus;
    }

    public static void closeSolrClient(SolrClient client) {
        try {
            if (null != client) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createSolrinputDocumentFromHBase(SolrInputDocument doc, Result result, String[] columns, String CF) {

        for (int i = 1; i < columns.length; i++) {
            String value = Bytes.toString(result.getValue(CF.getBytes(), columns[i].toUpperCase().getBytes()));
            if (StringUtils.isNotBlank(value)) {
                doc.addField(columns[i].toUpperCase(), value);
            }
            if (columns[i].equalsIgnoreCase("capture_time")) {
                try {
                    doc.addField("capture_time", timeFormat.parse(value).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
