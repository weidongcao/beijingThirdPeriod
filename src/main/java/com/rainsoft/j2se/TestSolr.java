package com.rainsoft.j2se;

import com.google.common.base.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {

    public static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    public static SolrClient client = (SolrClient) context.getBean("solrClient");
    public static Random random = new Random();
    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
//                doc.addField("CAPTURE_TIME", DateUtils.addHours(new Date(), random.nextInt(24)));
//                doc.addField("import_time", DateUtils.addHours(new Date(), random.nextInt(24)));
                doc.addField("import_time".toUpperCase(), new Timestamp(new Date().getTime()));
                doc.addField("capture_time".toUpperCase(), new Timestamp(new Date().getTime()));
                doc.addField("MSG", line);
                docs.add(doc);
            }
        }

        client.add(docs, 1000);
    }

    public void testMath() {
        System.out.println(Math.log(100d));
    }
    @Test
    public void insertTestData1() throws IOException, SolrServerException, ParseException {
        Optional<Date> now = Optional.of(df.parse("2018-03-06 12:00:00"));
        Optional<Date> oneWeekAge = Optional.of(df.parse("2018-02-28 12:00:00"));
        Optional<Date> twoWeekAge = Optional.of(df.parse("2018-02-22 12:00:00"));
        Optional<Date> twoMonthAge = Optional.of(df.parse("2018-01-06 12:00:00"));
        Optional<Date> oneYearAge = Optional.of(df.parse("2017-03-06 12:00:00"));
        Optional<Date> oneWeekAfter = Optional.of(df.parse("2018-03-11 12:00:00"));
        Optional<Date> oneMonthAfter = Optional.of(df.parse("2018-04-06 12:00:00"));
        Optional<Date> thirdMonthAfter = Optional.of(df.parse("2018-07-06 12:00:00"));
        Optional<Date> oneYearAfter = Optional.of(df.parse("2018-07-06 12:00:00"));
        String info100 = "我要买绝地武士星空仪";
        String info80 = "买绝地武士星空仪";
        String info40 = "黑武士星空仪";
        String info30 = "我要买小米";
        String infomm = "有问题请直接请 那还卖什么挂售后都没有";
        String chat = "聊天";
        String ftp = "文件";
        String real = "真实";
        String vid = "虚拟";
        String service = "场所";
        String fieldName1 = "MSG";
        String fieldName2 = "FILE_NAME";
        String fieldName3 = "SUMMARY";
        String fieldName4 = "KEYWORD";
        String fieldName5 = "URL";

        List<SolrInputDocument> docs = new ArrayList<>();

        docs.add(createDoc(chat, fieldName1, info100, now));
        // docs.add(createDoc(chat, fieldName1, info80, now));
        // docs.add(createDoc(chat, fieldName1, "武士要买星空绝地", now));

        //数据类型重要程序测试
        docs.add(createDoc(real, fieldName1, info100, Optional.absent()));
        docs.add(createDoc(vid, fieldName1, info100, Optional.absent()));
        docs.add(createDoc(service, fieldName1, info100, Optional.absent()));
        docs.add(createDoc(ftp, fieldName1, info100, now));

        //字段名重要程序测试
        docs.add(createDoc(chat, fieldName2, info100, now));
        docs.add(createDoc(chat, fieldName3, info100, now));
        docs.add(createDoc(chat, fieldName4, info100, now));
        docs.add(createDoc(chat, fieldName5, info100, now));

        //未来日期测试
        docs.add(createDoc(chat, fieldName1, info100, oneWeekAfter));
        docs.add(createDoc(chat, fieldName1, info100, oneMonthAfter));
        docs.add(createDoc(chat, fieldName1, info100, thirdMonthAfter));
        docs.add(createDoc(chat, fieldName1, info100, oneYearAfter));

        //匹配程度测试
        docs.add(createDoc(chat, fieldName1, info80, now));
        docs.add(createDoc(chat, fieldName1, info40, now));
        docs.add(createDoc(chat, fieldName1, info30, now));

        //日期排序测试
        docs.add(createDoc(chat, fieldName1, info100, twoWeekAge));
        docs.add(createDoc(chat, fieldName1, info100, twoMonthAge));
        docs.add(createDoc(chat, fieldName1, info100, oneYearAge));
        docs.add(createDoc(chat, fieldName1, info100, oneWeekAge));

        docs.add(createDoc(chat, fieldName1, infomm, now));
        docs.add(createDoc(chat, fieldName1, "www.baidu.com", now));
        docs.add(createDoc(chat, fieldName1, "ww.baidu.com", now));
        client.add(docs, 1000);
    }

    public static SolrInputDocument createDoc(String docType, String fieldName, String msg, Optional<Date> date){
        SolrInputDocument doc = new SolrInputDocument();
        String id = UUID.randomUUID().toString().replace("-", "");
        doc.addField("ID", id);
        doc.addField("docType", docType);
        doc.addField(fieldName, msg);
        if (date.isPresent()) {
            doc.addField("capture_time".toUpperCase(), df.format(date.get()));
            doc.addField("capture_time", date.get().getTime());
        }

        return doc;
    }
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
            System.out.println("import_time \t" + doc.get("import_time"));
            System.out.println("capture_time \t" + doc.get("capture_time"));
            System.out.println("  work_time \t" + doc.get("work_time"));
            break;
        }
        System.out.println("-----------------------");
    }
}