package com.rainsoft.j2se;

import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.utils.ReflectUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {
    public static void main(String[] args) throws IOException, SolrServerException {
        CloudSolrClient client = SolrUtil.getSolrClient("yisou");
        SolrInputDocument doc = new SolrInputDocument();
        RegContentFtp ftp = new RegContentFtp();
        ftp.setId("abcdefg");
        ftp.setCapture_time("2017-06-21 16:24:05");
        ftp.setAccount("test");

        Field[] fields = RegContentFtp.class.getFields();

        for (Field field : fields) {
            String fieldName = field.getName();
            String fieldValue = (String) ReflectUtils.getFieldValueByName(fieldName, ftp);
//            System.out.println(fieldName + " = " + fieldValue);
            doc.addField(fieldName.toUpperCase(), fieldValue);
        }

        client.add(doc);

        client.close();
    }
}
