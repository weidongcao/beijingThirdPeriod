package com.rainsoft.hbase.solr;

import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 将HBase的文件类型的临时表的数据导入到Solr
 * Created by CaoWeiDong on 2017-09-24.
 */
public class FtpImportSolrBase {
    private static final Logger logger = LoggerFactory.getLogger(FtpImportSolrBase.class);

    private static SolrClient client = SolrUtil.getClusterSolrClient();
    private static int batchCount = ConfigurationManager.getInteger("commit.solr.count");
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_content_ftp");
    private static String TABLE_NAME = "H_REG_CONTENT_FTP_TMP";
    private static final String CF = "CONTENT_FTP";
    private static final String taskType = "ftp";

    public static void main(String[] args) throws Exception {
        Table table = null;
        ResultScanner resultScanner = null;
        List<SolrInputDocument> docList = new ArrayList<>();
        table = HBaseUtils.getTable(TABLE_NAME);

        Scan scan = new Scan();
        resultScanner = table.getScanner(scan);

        // iterator
        for (Result result : resultScanner) {
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");
            doc.addField("ID", uuid);
            doc.addField("docType", "文件");
            doc.addField("SID", Bytes.toString(result.getRow()));

            //从Solr取出来的数据封装到Solr
            SolrUtil.createSolrinputDocumentFromHBase(doc, result, columns, CF);
            docList.add(doc);
            if (!docList.isEmpty() && (docList.size() >= batchCount)) {
                client.add(docList, 10000);
                logger.info("{} 写入Solr {} 数据成功...", taskType, docList.size());
                docList.clear();
            }
        }
        if (!docList.isEmpty()) {
            logger.info("{} 写入Solr {} 数据成功...", taskType, docList.size());
            client.add(docList, 10000);
        }
        IOUtils.closeStream(resultScanner);
        IOUtils.closeStream(table);
    }
}
