package com.rainsoft.bcp;

import com.rainsoft.utils.SolrUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;

/**
 * Created by CaoWeiDong on 2017-09-15.
 */
public class TestSolrConnection {
    public static SolrClient client = SolrUtil.getClusterSolrClient();
    public static void main(String[] args) {
        String q = args[0];
        querySolr(q);
    }

    public static void querySolr(String q){

        SolrQuery params = new SolrQuery();
        params.set("q", q);
        params.set("start", 0);
        params.set("rows", 5);
        QueryResponse rsp = null;
        try {
            rsp = client.query(params);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        SolrDocumentList docs = rsp.getResults();
        System.out.println("文档数量： " + docs.getNumFound());
        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            // 多值查询
            @SuppressWarnings("unchecked")
            String ID = (String) doc.getFieldValue("ID");
            String SID = (String) doc.getFieldValue("SID");
            String subject = (String)
                    doc.getFieldValue("SUBJECT");
            System.out.println("ID:" + ID
                    + "\t  SID:"
                    + SID + "\t  SUBJECT" + subject);
        }
        System.out.println("-----------------------");
    }
}
