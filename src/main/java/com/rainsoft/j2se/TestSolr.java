package com.rainsoft.j2se;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {
    public static CloudSolrClient client;

    static {
        //Zookeeper节点主机名
        String zkHost = "dn1.hadoop.com," +
                "dn2.hadoop.com," +
                "dn3.hadoop.com," +
                "dn4.hadoop.com," +
                "nn2.hadoop.com";
        //通过Zookeeper获取Solr连接
        client = new CloudSolrClient
                .Builder()
                .withZkHost(zkHost)
                .build();
        //指定要连接的Solr的collection
        client.setDefaultCollection(
                ConfigurationManager
                        .getProperty("solr.collection")
        );
    }

    public static void main(String[] args)
            throws IOException, SolrServerException {
        //从Solr查询数据
        querySolr(
                "docType:\"网页\" " +
                "USER_NAME:\"郭晓静\" " +
                "REF_DOMAIN:\"speed.qq.com\" " +
                "DEST_IP:\"3722762918\""
        );
        //向Solr插入或者更新索引的例子
        insertOrupdateSolr();

        //删除Solr的索引
        deleteByQuery(
                "docType:\"网页\" " +
                "USER_NAME:\"郭晓静\" " +
                "REF_DOMAIN:\"speed.qq.com\" " +
                "DEST_IP:\"3722762918\""
        );
        // 关闭连接
        client.close();
    }

    public static void insertOrupdateSolr()
            throws IOException, SolrServerException {
        //当前时间
        Date curDate = new Date();
        //生成要插入Solr的数据
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String rowKey = UUID.randomUUID()
                    .toString().replace("-", "")
                    + "_" + curDate.getTime();
            String captureTimeMinSecond = rowKey.split("_")[0];
            String id = rowKey.split("_")[1];
            SolrInputDocument doc = new SolrInputDocument();
            //仅有的字段
            doc.addField("ID".toUpperCase(), id);
            doc.addField("SID".toUpperCase(), rowKey);
            doc.addField("docType", "网页");
            doc.addField(
                    "IMPORT_TIME".toUpperCase()
                    , DateUtils.TIME_FORMAT.format(curDate)
            );
            doc.addField("import_time", curDate.getTime());
            doc.addField(
                    "capture_time".toLowerCase()
                    , Long.valueOf(captureTimeMinSecond)
            );
            //独有的字段
            //TODO
            docs.add(doc);
        }
        //在1分钟内自动提交到Solr
        client.add(docs, 1000);
    }

    public static void deleteByQuery(String query) {
        try {
            client.deleteByQuery(query);
            client.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void querySolr(String q)
            throws IOException, SolrServerException {
        SolrQuery params = new SolrQuery();
        params.set("q", q);
        params.set("start", 0);
        params.set("rows", 5);
        QueryResponse rsp = client.query(params);
        SolrDocumentList docs = rsp.getResults();
        System.out.println("文档数量：" + docs.getNumFound());
        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            // 多值查询
            @SuppressWarnings("unchecked")
            String ID = (String) doc.getFieldValue("ID");
            String SID = (String) doc.getFieldValue("SID");
            String subject = (String)
                    doc.getFieldValue("SUBJECT");
            System.out.println("ID:" + ID
                    + "\t SID:"
                    + SID + "\t SUBJECT" + subject);
        }
        System.out.println("-----------------------");
    }
}