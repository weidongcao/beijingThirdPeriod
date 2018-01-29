package com.rainsoft.j2se;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateFormatUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {

    public static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    public static SolrClient client = (SolrClient) context.getBean("solrClient");
    public static Random random = new Random();

    static {
//        String solrURL = ConfigurationManager.getProperty("solr.url");
//        client = new HttpSolrClient.Builder(solrURL).build();
    }

    public void insertTestData() throws IOException, SolrServerException {
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
//                doc.addField("CAPTURE_TIME", DateUtils.addHours(new Date(), random.nextInt(24)));
//                doc.addField("import_time", DateUtils.addHours(new Date(), random.nextInt(24)));
                doc.addField("import_time".toUpperCase(), new Timestamp(new Date().getTime()));
                doc.addField("capture_time".toUpperCase(), new Timestamp(new Date().getTime()));
                doc.addField("MSG", "dkjf");
                docs.add(doc);
            }
        }

        client.add(docs, 1000);
    }

    @Test
    public void querySolr()
            throws IOException, SolrServerException {
        SolrQuery params = new SolrQuery();
        params.setQuery("*:*");
        params.setStart(0);
        params.setRows(60);

        QueryResponse rsp = client.query(params);
        SolrDocumentList docs = rsp.getResults();
        System.out.println("文档数量：" + docs.getNumFound());
        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            System.out.println("   cur_date \t" + doc.get("cur_date"));
            System.out.println("import_time \t" +  doc.get("import_time"));
            System.out.println("capture_time \t" + doc.get("capture_time"));
            System.out.println("  work_time \t" + doc.get("work_time"));
            break;
        }
        System.out.println("-----------------------");
    }
}