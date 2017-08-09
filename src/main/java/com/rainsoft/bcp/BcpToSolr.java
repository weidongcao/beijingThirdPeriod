package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * BCP文件写入Solr并写入本地tsv文件
 * Created by CaoWeiDong on 2017-08-09.
 */
public class BcpToSolr {
    private static final Logger logger = LoggerFactory.getLogger(BcpToSolr.class);
    private static final String baseBcpPath = ConfigurationManager.getProperty("bcp_file_path");
    private static final CloudSolrClient client = SolrUtil.getClusterSolrClient();


    public static void main(String[] args) throws IOException {
        String taskType = "http";
        int captureTimeIndex = 17;

        String[] fieldNames = FieldConstants.BCP_FIELD_MAP.get(taskType);
        File dataDir = FileUtils.getFile(baseBcpPath + File.separator + taskType);
        int maxFileDataSize = ConfigurationManager.getInteger("max_oracle_data_file_lines");
        Date curDate = new Date();

        File[] dataFileList = dataDir.listFiles();

        StringBuffer sb = new StringBuffer();
        List<SolrInputDocument> docList = new ArrayList<>();
        int lineCount = 0;
        if (null != dataFileList) {
            for (File file :
                    dataFileList) {
                List<String> lines = FileUtils.readLines(file, "utf-8");
                for (String line :
                        lines) {
                    String[] fieldValues = line.replace(BigDataConstants.BCP_LINE_SEPARATOR, "")
                            .replace("\t", "")
                            .split(BigDataConstants.BCP_FIELD_SEPARATOR);

                    if ((fieldNames.length + 1 == fieldValues.length) ||     //BCP文件 没有新加字段
                            ((fieldNames.length + 1) == (fieldValues.length + 3)) ||     //BCP文件添加了新的字段，且只添加了三个字段
                            (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(taskType) &&
                                    ((fieldNames.length + 1) == (fieldValues.length + 3 + 4)))) {    //HTTP的BCP文件添加了新的字段，且添加了7个字段
                        long captureTimeMinSecond;
                        try {
                            captureTimeMinSecond = DateUtils.TIME_FORMAT.parse(fieldValues[captureTimeIndex]).getTime();
                        } catch (Exception e) {
                            continue;
                        }
                        if (captureTimeMinSecond > curDate.getTime()) {
                            logger.warn("脏数据,捕获日期大于当前日期,捕获日期为：{}", fieldValues[captureTimeIndex]);
                            logger.warn("此条数据全部信息为: {}", line);
                            continue;
                        }
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = captureTimeMinSecond + "_" + uuid;
                        sb.append(rowkey).append("\t");

                        SolrInputDocument doc = new SolrInputDocument();
                        doc.addField("ID".toUpperCase(), uuid);
                        doc.addField("SID".toUpperCase(), rowkey);
                        doc.addField("docType", FieldConstants.DOC_TYPE_MAP.get(taskType));
                        doc.addField("IMPORT_TIME".toUpperCase(), DateUtils.TIME_FORMAT.format(curDate));
                        doc.addField("capture_time".toLowerCase(), captureTimeMinSecond + "");

                        for (int i = 0; i < fieldValues.length; i++) {
                            String fieldValue = fieldValues[i];

                            doc.addField(fieldNames[i], fieldValue);

                            if (i < (fieldValues.length - 1)) {
                                sb.append(fieldValue).append("\t");
                            } else {
                                sb.append(fieldValue);
                            }
                        }
                        lineCount++;

                        if (lineCount >= maxFileDataSize) {
                            //写入Solr
                            SolrUtil.submit(docList, client);
                            logger.info("写入Solr {} 的 {} 数据成功", lineCount, taskType);
                            docList.clear();
                            //写入本地文件
                            String fileName = writeStringToFile(sb, taskType);
                            logger.info("写入本地数据文件成功 : {}", fileName);
                            sb = new StringBuffer();
                            lineCount = 0;
                        }
                    }
                }
                if (docList.size() > 0) {
                    //write into solr
                    SolrUtil.submit(docList, client);
                    logger.info("写入Solr {} 的 {} 数据成功", lineCount, taskType);
                    //write into local file
                    String fileName = writeStringToFile(sb, taskType);
                    logger.info("写入本地数据文件成功 : {}", fileName);
                }
            }
            logger.info("{}类型的BCP数据处理写成", taskType);
        } else {
            logger.info("没有{}类型的BCP数据需要处理", taskType);
        }


    }

    /**
     * 将数据写入到文件
     *
     * @param sb
     */
    private static String writeStringToFile(StringBuffer sb, String taskType) {
        String constantPath = ConfigurationManager.getProperty("load_data_workspace");
        //${basePath}/${workType}/${dataType}/${tableDataFileName}
        String filePathTemplate = constantPath + File.separator     //${basePath}
                + "${workType}" + File.separator         //${workType}
                + "bcp-${taskType}" + File.separator;        //${workType}
        String fileNameTemplate = "bcp-${taskType}_data_${timestamp}_${random}.tsv";

        String random = RandomStringUtils.randomAlphabetic(8);
        String filePath = filePathTemplate.replace("${workType}", "work")
                .replace("${taskType}", taskType);
        String fileName = fileNameTemplate.replace("${taskType}", taskType)
                .replace("${timestamp}", DateUtils.STEMP_FORMAT.format(new Date()))
                .replace("${random}", random);

        File parentDir = new File(filePath);
        File dataFile = new File(parentDir, fileName);
        if (!parentDir.exists() || !parentDir.isDirectory()) {
            parentDir.mkdirs();
        }
        if (dataFile.exists() || dataFile.isDirectory()) {
            dataFile.delete();
        }

        try {
            dataFile.createNewFile();
            FileUtils.writeStringToFile(dataFile, sb.toString(), "utf-8", false);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("生产数据文件失败...");
            System.exit(-1);
        }

        return fileName;
    }
}
