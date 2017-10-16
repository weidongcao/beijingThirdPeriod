package com.rainsoft.hbase.solr;

import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SolrUtil;
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
 * 将从HBase的网页类型的临时表里取出的数据导入到Solr
 * Created by CaoWeiDong on 2017-10-15.
 */
public class HttpImportSolrBase {
    private static final Logger logger = LoggerFactory.getLogger(HttpImportSolrBase.class);

    private static SolrClient client = SolrUtil.getClusterSolrClient();
    private static int batchCount = ConfigurationManager.getInteger("commit.solr.count");
    private static final String task = "http";
    private static String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName(task));
    private static String TABLE_NAME = NamingRuleUtils.getTmpHBaseTableName(task);
    private static final String CF = NamingRuleUtils.getHBaseContentTableCF(task);

    public static void main(String[] args) throws Exception {
        //获取HBase表
        Table table = HBaseUtils.getTable(TABLE_NAME);
        //获取HBase的scan
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        //要返回的Solr写入列表
        List<SolrInputDocument> docList = new ArrayList<>();

        // 将从HBase查询出来的数据转为Solr的写入列表
        for (Result result : resultScanner) {
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");
            doc.addField("ID", uuid);
            doc.addField("docType", "网页");
            doc.addField("SID", Bytes.toString(result.getRow()));

            //从Solr取出来的数据封装到Solr
            SolrUtil.createSolrinputDocumentFromHBase(doc, result, columns, CF);
            docList.add(doc);
            if (!docList.isEmpty() && (docList.size() >= batchCount)) {
                client.add(docList, 10000);
                logger.info("{} 写入Solr {} 数据成功...", task, docList.size());
                docList.clear();
            }
        }
        if (!docList.isEmpty()) {
            logger.info("{} 写入Solr {} 数据成功...", task, docList.size());
            client.add(docList, 10000);
        }
        IOUtils.closeStream(resultScanner);
        IOUtils.closeStream(table);
    }
}
