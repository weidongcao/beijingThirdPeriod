package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.domain.TaskBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *  创建任务将BCP数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-07-30.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String tmpHFilePath = BigDataConstants.TMP_HFILE_HDFS;
    private static final String tsvDataPathTemplate = "file://" + ConfigurationManager.getProperty("load_data_workspace") + "/work/bcp-${task}";
//    private static final String tsvDataPathTemplate = "file:///" + ConfigurationManager.getProperty("bcp_file_path") + File.separator;
    private static final String hbaseTablePrefix = BigDataConstants.HBASE_TABLE_PREFIX;
    public static void main(String[] args) {
        String type = args[0];

        if (BigDataConstants.CONTENT_TYPE_FTP.equalsIgnoreCase(type)) {
            SparkOperateBcp.run(getFtpTask());
        } else if (BigDataConstants.CONTENT_TYPE_IM_CHAT.equalsIgnoreCase(type)) {
            SparkOperateBcp.run(getImchatTask());
        } else if (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(type)) {
            SparkOperateBcp.run(getHttpTask());
        }
        logger.info("############ 数据处理完成 ############");
    }

    /**
     * ftp的任务
     * @return
     */
    private static TaskBean getFtpTask() {
        String oracleTableName = BigDataConstants.ORACLE_TABLE_FTP_NAME;
        String contentType = BigDataConstants.CONTENT_TYPE_FTP;
        TaskBean ftp = new TaskBean();
        ftp.setBcpPath(tsvDataPathTemplate.replace("${task}",contentType));
        ftp.setCaptureTimeIndex(17);
        ftp.setContentType(contentType);
        ftp.setDocType(BigDataConstants.SOLR_DOC_TYPE_FTP_VALUE);
        ftp.setColumns(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_FTP));
        ftp.setHbaseCF(BigDataConstants.HBASE_TABLE_FTP_CF);
        ftp.setHfileTmpStorePath(tmpHFilePath + contentType);
        ftp.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());

        return ftp;
    }

    /**
     *  聊天的任务
     * @return
     */
    private static TaskBean getImchatTask() {
        String contentType = BigDataConstants.CONTENT_TYPE_IM_CHAT;
        String oracleTableName = BigDataConstants.ORACLE_TABLE_IM_CHAT_NAME;
        TaskBean imChat = new TaskBean();
        imChat.setBcpPath(tsvDataPathTemplate.replace("${task}",contentType));
        imChat.setCaptureTimeIndex(20);
        imChat.setContentType(contentType);
        imChat.setDocType(BigDataConstants.SOLR_DOC_TYPE_IMCHAT_VALUE);
        imChat.setColumns(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_IM_CHAT));
        imChat.setHfileTmpStorePath(tmpHFilePath + contentType);
        imChat.setHbaseCF(BigDataConstants.HBASE_TABLE_IM_CHAT_CF);
        imChat.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());

        String path = ConfigurationManager.getProperty("bcp_file_path") + File.separator + contentType;
        logger.info("替换 {} 的BCP数据的目录： {}", imChat.getContentType(), path);
//        imChat.replaceFileRN(path);
        return imChat;
    }

    /**
     * 网页的任务
     * @return
     */
    private static TaskBean getHttpTask() {
        String contentType = BigDataConstants.CONTENT_TYPE_HTTP;
        String oracleTableName = BigDataConstants.ORACLE_TABLE_HTTP_NAME;
        TaskBean http = new TaskBean();
        http.setBcpPath(tsvDataPathTemplate.replace("${task}",contentType));
        http.setCaptureTimeIndex(22);
        http.setContentType(contentType);
        http.setDocType(BigDataConstants.SOLR_DOC_TYPE_HTTP_VALUE);
        http.setColumns(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_HTTP));
        http.setHfileTmpStorePath(tmpHFilePath + contentType);
        http.setHbaseCF(BigDataConstants.HBASE_TABLE_HTTP_CF);
        http.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());

        return http;
    }

}
