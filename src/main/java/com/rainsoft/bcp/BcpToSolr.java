package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * BCP文件写入Solr并写入本地tsv文件
 * Created by CaoWeiDong on 2017-08-09.
 */
public class BcpToSolr {
    private static final Logger logger = LoggerFactory.getLogger(BcpToSolr.class);
    private static final String basePath = ConfigurationManager.getProperty("load.data.workspace") + File.separator + "work";
    private static final SolrClient client = SolrUtil.getClusterSolrClient();

    public static void main(String[] args) throws IOException, SolrServerException {
        logger.info("开始将BCP数据索引到Solr...");
        String taskType = args[0];

        String taskDataDir = "bcp-" + taskType;
        //获取字段名数组
        String[] fieldNames = FieldConstants.COLUMN_MAP.get("bcp_" + taskType);
        //数据文件所在目录
        File dataDir = FileUtils.getFile(basePath + File.separator + taskDataDir);
        logger.info("转存的TSV文件所在目录:{}", dataDir.getAbsolutePath());
        //一次最多向Solr提交5万条数据
        int maxFileDataSize = ConfigurationManager.getInteger("commit.solr.count");

        //当前时间
        Date curDate = new Date();

        //数据文件
        File[] dataFileList = dataDir.listFiles();

        //要写入Solr的数据集合
        List<SolrInputDocument> docList = new ArrayList<>();
        if (null != dataFileList) {
            for (File file : dataFileList) {
                List<String> lines = FileUtils.readLines(file, "utf-8");
                for (String line : lines) {
                    //一条数据切分成多个字段的值
                    String[] fieldValues = line.split("\t");

                    //有多少个字段值
                    int fieldValuesLength = fieldValues.length;
                    //标准有多少个字段名
                    int fieldNamesLength = fieldNames.length;

                    if ((fieldNamesLength + 1 == fieldValuesLength) ||     //BCP文件是最新的版本(最新版本的BCP文件HTTP增加了7个字段，其他的增加了3个字段)
                            (fieldNamesLength + 1 == (fieldValuesLength + 3)) ||     //BCP文件不是最新的版本(没有升级)，且只添加了三个字段
                            (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(taskType) &&
                                    (fieldNamesLength + 1 == (fieldValuesLength + 3 + 4)))) {    //BCP文件不是最新的版本(没有升级)，BCP文件是http数据，没有增加7个字段

                        String rowKey = fieldValues[0];
                        String captureTimeMinSecond = rowKey.split("_")[0];
                        String id = rowKey.split("_")[1];

                        SolrInputDocument doc = new SolrInputDocument();
                        doc.addField("ID".toUpperCase(), id);
                        doc.addField("SID".toUpperCase(), rowKey);
                        doc.addField("docType", FieldConstants.DOC_TYPE_MAP.get(taskType));
                        doc.addField("IMPORT_TIME".toUpperCase(), DateFormatUtils.DATE_TIME_FORMAT.format(curDate));
                        doc.addField("import_time", curDate.getTime());
                        doc.addField("capture_time".toLowerCase(), Long.valueOf(captureTimeMinSecond) );

                        for (int i = 1; i < fieldValues.length; i++) {
                            String value = fieldValues[i];
                            String key = fieldNames[i -1].toUpperCase();
                            //如果字段的值为空则不写入Solr
                            if ((null != value) && (!"".equals(value))) {
                                if (!"FILE_URL".equalsIgnoreCase(key) && !"FILE_SIZE".equalsIgnoreCase(key)) {
                                    doc.addField(key, value);
                                }
                            }
                        }
                        docList.add(doc);

                        if (docList.size() >= maxFileDataSize) {
                            //写入Solr
                             client.add(docList, 1000);
                            logger.info(" {} 写入Solr {} 数据成功", taskType, docList.size());
                            docList.clear();
                        }
                    }
                }
            }
            if (docList.size() > 0) {
                //write into solr
                client.add(docList, 1000);
                logger.info("{} 写入Solr {} 数据成功", taskType, docList.size());
            }
            logger.info("{}类型的BCP数据处理写成", taskType);
        } else {
            logger.info("没有{}类型的BCP数据需要处理", taskType);
        }

        SolrUtil.closeSolrClient(client);
    }
}
