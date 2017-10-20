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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by CaoWeiDong on 2017-09-24.
 */
public class RealidImportSolrBase {
    private static final Logger logger = LoggerFactory.getLogger(RealidImportSolrBase.class);

    private static SolrClient client = SolrUtil.getClusterSolrClient();
    private static int batchCount = ConfigurationManager.getInteger("commit.solr.count");
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_realid_info");
    private static String TABLE_NAME = "H_REG_REALID_INFO_TMP";
    private static final String CF = "REALID_INFO";
    private static final String taskType = "realid";

    public static void main(String[] args) throws Exception {
        Table table = HBaseUtils.getTable(TABLE_NAME);
        List<SolrInputDocument> docList = new ArrayList<>();

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        // iterator
        for (Result result : resultScanner) {
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");
            doc.addField("ID", uuid);
            doc.addField("docType", "真实");
            doc.addField("SID", Bytes.toString(result.getRow()));

            for (int i = 1; i < columns.length; i++) {
                String value = Bytes.toString(result.getValue(CF.getBytes(), columns[i].toUpperCase().getBytes()));
                if (StringUtils.isNotBlank(value)) {
                    doc.addField(columns[i].toUpperCase(), value);
                }
            }
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
