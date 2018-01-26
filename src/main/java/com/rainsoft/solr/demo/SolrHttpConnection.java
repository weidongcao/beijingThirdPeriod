package com.rainsoft.solr.demo;

import com.rainsoft.utils.SolrUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

/**
 * Created by CaoWeiDong on 2017-12-21.
 */
public class SolrHttpConnection {
    public static final HttpSolrClient client = new HttpSolrClient.Builder("http://cm02.spark.com:8983/solr/yisou").build();
    public static void main(String[] args) throws IOException, SolrServerException {
        querySolr();
    }
    public static void insertSolr() throws IOException, SolrServerException {
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        for (int i = 1; i <= 1; i++) {
            SolrInputDocument doc1 = new SolrInputDocument();
            //公有的字段
            doc1.addField("id", UUID.randomUUID().toString(), 1.0f);
            doc1.addField("capture_dt".toUpperCase(), new Date());

            //独有的字段
            //TODO

            docs.add(doc1);
        }
        SolrUtil.setCloudSolrClientDefaultCollection(client);
        client.add(docs);
        client.commit();
    }
    public static void querySolr()
            throws IOException, SolrServerException {
        SolrQuery params = new SolrQuery();
//        params.set("q", "我要买绝地武士星空仪");
        params.setQuery("*:*");
        params.setStart(0);
        params.setRows(20);
//        params.set

        QueryResponse rsp = client.query(params);
        SolrDocumentList docs = rsp.getResults();
        System.out.println("文档数量：" + docs.getNumFound());
        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            System.out.println(doc.toString());
        }
        System.out.println("-----------------------");
    }
}
