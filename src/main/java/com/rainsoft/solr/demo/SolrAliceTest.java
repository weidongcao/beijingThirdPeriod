package com.rainsoft.solr.demo;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Created by CaoWeiDong on 2017-12-14.
 */
public class SolrAliceTest {
    public static final String zkhost = "data1.hadoop.com,data2.hadoop.com,data3.hadoop.com,node1.hadoop.com,node2.hadoop.com";
    public static CloudSolrClient client = new CloudSolrClient.Builder().withZkHost(zkhost).build();

    public static void main(String[] args) throws IOException, SolrServerException {
//        instertData();
//        createAlias();
//        query();

        getAllCollection();
    }
    public static void getAllCollection() throws IOException, SolrServerException {
        CollectionAdminRequest.List list = CollectionAdminRequest.listCollections();
        CollectionAdminResponse response = list.process(client);
        System.out.println("response = " + response);
        client.close();

    }
    public static void createAlias() throws IOException, SolrServerException {
        String collections = "yisou20171200,yisou20171201,yisou20171202";
//        instertData();
        CollectionAdminRequest.CreateAlias alias = CollectionAdminRequest.createAlias("yisou", collections);
        CollectionAdminResponse response = alias.process(client);
        System.out.println("response = " + response);
    }

    public static void instertData() throws IOException, SolrServerException {
        client.setDefaultCollection("yisou20171200");
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        SolrInputDocument doc1 = new SolrInputDocument();
        //公有的字段
        doc1.addField("id".toUpperCase(), UUID.randomUUID().toString(), 1.0f);
        doc1.addField("SID", "aaaaaa");
        doc1.addField("docType", "网页");
        doc1.addField("MSG".toUpperCase(), "森森木");
        //独有的字段
        //TODO
        docs.add(doc1);
        client.add(docs, 10000);
        docs.clear();


        client.setDefaultCollection("yisou20171201");
        doc1 = new SolrInputDocument();
        //公有的字段
        doc1.addField("id".toUpperCase(), UUID.randomUUID().toString(), 1.0f);
        doc1.addField("SID", "bbbb");
        doc1.addField("docType", "文件");
        doc1.addField("MSG".toUpperCase(), "品吕口");
        //独有的字段
        //TODO
        docs.add(doc1);
        client.add(docs, 10000);


        client.setDefaultCollection("yisou20171202");
        doc1 = new SolrInputDocument();
        //公有的字段
        doc1.addField("id".toUpperCase(), UUID.randomUUID().toString(), 1.0f);
        doc1.addField("SID", "cccccc");
        doc1.addField("docType", "聊天");
        doc1.addField("MSG".toUpperCase(), "哈哈");
        //独有的字段
        //TODO
        docs.add(doc1);
        client.add(docs, 10000);
    }

    public static void query() throws IOException, SolrServerException {
        client.setDefaultCollection("yisou");

        SolrQuery params = new SolrQuery();
        System.out.println("======================query===================");
        params.set("q", "*:*");
        params.set("start", 0);
        params.set("rows", 5);

        QueryResponse rsp = client.query(params);
        SolrDocumentList docs = rsp.getResults();
        System.out.println("查询内容:" + params);
        System.out.println("文档数量：" + docs.getNumFound());
        System.out.println("查询花费时间:" + rsp.getQTime());

        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            System.out.println("doc = " + doc.toString());
        }
        System.out.println("-----------------------");
    }
}
