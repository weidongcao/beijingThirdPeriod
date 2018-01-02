package com.rainsoft.j2se;

import com.rainsoft.utils.DateFormatUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {

    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    public static SolrClient client = (SolrClient) context.getBean("solrClient");

    public static void main(String[] args)
            throws IOException, SolrServerException {
        //从Solr查询数据
        querySolr();
//        insertTestData();
    }

    public static void insertTestData() throws IOException, SolrServerException {
        File file = FileUtils.getFile("D:\\opt\\aaa.txt");
        List<String> list = FileUtils.readLines(file, "utf-8");
        List<SolrInputDocument> docs = new ArrayList<>();
        for (String line : list) {
            if (StringUtils.isNotBlank(line)) {

                SolrInputDocument doc = new SolrInputDocument();
                String id = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", id);
                doc.addField("docType", "聊天");
                doc.addField("SUMMARY", line);
                doc.addField("MSG", "");
                docs.add(doc);
            }
        }

        client.add(docs, 1000);
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
                    , DateFormatUtils.DATE_TIME_FORMAT.format(curDate)
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

    public static void querySolr()
            throws IOException, SolrServerException {
        SolrQuery params = new SolrQuery();
//        params.set("q", "我要买绝地武士星空仪");
//        params.set("start", 0);
//        params.set("rows", 20);
        params.setQuery("我要买绝地武士星空仪");
        params.setStart(0);
        params.setRows(60);
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