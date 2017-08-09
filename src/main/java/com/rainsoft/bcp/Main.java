package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.JsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.SchemaOutputResolver;
import java.io.File;

/**
 *  创建任务将BCP数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-07-30.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String tmpHFilePath = BigDataConstants.TMP_HFILE_HDFS;
    private static final String bcpPath = "file://" + ConfigurationManager.getProperty("bcp_file_path") + File.separator;
//    private static final String bcpPath = "file:///" + ConfigurationManager.getProperty("bcp_file_path") + File.separator;
    private static final String hbaseTablePrefix = BigDataConstants.HBASE_TABLE_PREFIX;
    public static void main(String[] args) {
        String type = args[0];
        SparkConf conf = new SparkConf()
                .setAppName("import bcp to solr and hbase");
        JavaSparkContext sc = new JavaSparkContext(conf);

        if (BigDataConstants.CONTENT_TYPE_FTP.equalsIgnoreCase(type)) {
            getFtpJob().run(sc);
        } else if (BigDataConstants.CONTENT_TYPE_IM_CHAT.equalsIgnoreCase(type)) {
            getImchatJob().run(sc);
        } else if (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(type)) {
            getHttpJob().run(sc);
        }
        sc.stop();
        logger.info("############ 数据处理完成 ############");
        System.exit(0);
    }

    public static SparkOperateBcp getFtpJob() {
        String oracleTableName = BigDataConstants.ORACLE_TABLE_FTP_NAME;
        String contentType = BigDataConstants.CONTENT_TYPE_FTP;
        SparkOperateBcp ftp = new SparkOperateBcp();
        ftp.setBcpPath(bcpPath + contentType);
        ftp.setCaptureTimeIndexBcpFileLine(17);
        ftp.setContentType(contentType);
        ftp.setDocType(BigDataConstants.SOLR_DOC_TYPE_FTP_VALUE);
        ftp.setFields(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_FTP));
        ftp.setHbaseCF(BigDataConstants.HBASE_TABLE_FTP_CF);
        ftp.setHfileTmpStorePath(tmpHFilePath + contentType);
        ftp.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());
        return ftp;
    }

    public static SparkOperateBcp getImchatJob() {
        String contentType = BigDataConstants.CONTENT_TYPE_IM_CHAT;
        String oracleTableName = BigDataConstants.ORACLE_TABLE_IM_CHAT_NAME;
        SparkOperateBcp imChat = new SparkOperateBcp();
        imChat.setBcpPath(bcpPath + contentType);
        imChat.setCaptureTimeIndexBcpFileLine(20);
        imChat.setContentType(contentType);
        imChat.setDocType(BigDataConstants.SOLR_DOC_TYPE_IMCHAT_VALUE);
        imChat.setFields(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_IM_CHAT));
        imChat.setHfileTmpStorePath(tmpHFilePath + contentType);
        imChat.setHbaseCF(BigDataConstants.HBASE_TABLE_IM_CHAT_CF);
        imChat.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());

        String path = ConfigurationManager.getProperty("bcp_file_path") + File.separator + contentType;
        logger.info("替换 {} 的BCP数据的目录： {}", imChat.getContentType(), path);
        imChat.replaceFileRN(path);
        return imChat;
    }

    public static SparkOperateBcp getHttpJob() {
        String contentType = BigDataConstants.CONTENT_TYPE_HTTP;
        String oracleTableName = BigDataConstants.ORACLE_TABLE_HTTP_NAME;
        SparkOperateBcp http = new SparkOperateBcp();
        http.setBcpPath(bcpPath + contentType);
        http.setCaptureTimeIndexBcpFileLine(22);
        http.setContentType(contentType);
        http.setDocType(BigDataConstants.SOLR_DOC_TYPE_HTTP_VALUE);
        http.setFields(FieldConstants.BCP_FIELD_MAP.get(BigDataConstants.CONTENT_TYPE_HTTP));
        http.setHfileTmpStorePath(tmpHFilePath + contentType);
        http.setHbaseCF(BigDataConstants.HBASE_TABLE_HTTP_CF);
        http.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());

        return http;
    }

}
