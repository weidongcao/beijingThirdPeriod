package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
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
    private static final String basePath = ConfigurationManager.getProperty("load_data_workspace") + File.separator + "work";
    private static final CloudSolrClient client = SolrUtil.getClusterSolrClient();


    public static void main(String[] args) throws IOException {
        String taskType = "ftp";
        int captureTimeIndex = 17;

        String taskDataDir = "bcp-" + taskType;
        //获取字段名数组
        String[] fieldNames = FieldConstants.BCP_FIELD_MAP.get(taskType);
        //数据文件所在目录
        File dataDir = FileUtils.getFile(basePath + File.separator + taskDataDir);
        //一次最多向Solr提交多少条数据
        int maxFileDataSize = ConfigurationManager.getInteger("data_file_max_lines");
        //当前时间
        Date curDate = new Date();

        //数据文件
        File[] dataFileList = dataDir.listFiles();

        //要写入Solr的数据集合
        List<SolrInputDocument> docList = new ArrayList<>();
        if (null != dataFileList) {
            for (File file :
                    dataFileList) {

                List<String> lines = FileUtils.readLines(file, "utf-8");
                for (String line :
                        lines) {

                    //一条数据切分成多个字段的值
                    String[] fieldValues = line.split("\t");

                    //有多少个字段值
                    int fieldValuesLength = fieldValues.length;
                    //标准有多少个字段名
                    int fieldNamesLength = fieldNames.length;

                    if ((fieldNamesLength == fieldValuesLength) ||     //BCP文件是最新的版本(最新版本的BCP文件HTTP增加了7个字段，其他的增加了3个字段)
                            (fieldNamesLength == (fieldValuesLength + 3)) ||     //BCP文件不是最新的版本(没有升级)，且只添加了三个字段
                            (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(taskType) &&
                                    (fieldNamesLength == (fieldValuesLength + 3 + 4)))) {    //BCP文件不是最新的版本(没有升级)，BCP文件是http数据，没有增加7个字段

                        String rowkey = fieldValues[0];
                        String captureTimeMinSecond = rowkey.split("_")[0];
                        String id = rowkey.split("_")[1];

                        SolrInputDocument doc = new SolrInputDocument();
                        doc.addField("ID".toUpperCase(), id);
                        doc.addField("SID".toUpperCase(), rowkey);
                        doc.addField("docType", FieldConstants.DOC_TYPE_MAP.get(taskType));
                        doc.addField("IMPORT_TIME".toUpperCase(), DateUtils.TIME_FORMAT.format(curDate));
                        doc.addField("capture_time".toLowerCase(), captureTimeMinSecond );

                        for (int i = 0; i < fieldValues.length; i++) {
                            doc.addField(fieldNames[i], fieldValues[i]);
                        }
                        docList.add(doc);

                        if (docList.size() >= maxFileDataSize) {
                            //写入Solr
                            SolrUtil.submit(docList, client);
                            logger.info("写入Solr {} 的 {} 数据成功", docList.size(), taskType);
                            docList.clear();
                        }
                    }
                }
                if (docList.size() > 0) {
                    //write into solr
                    SolrUtil.submit(docList, client);
                    logger.info("写入Solr {} 的 {} 数据成功", docList.size(), taskType);
                }
            }
            logger.info("{}类型的BCP数据处理写成", taskType);
        } else {
            logger.info("没有{}类型的BCP数据需要处理", taskType);
        }


    }
}