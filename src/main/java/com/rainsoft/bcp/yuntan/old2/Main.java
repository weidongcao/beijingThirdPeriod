package com.rainsoft.bcp.yuntan.old2;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.domain.TaskBean;
import com.rainsoft.utils.NamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *  创建任务将BCP数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-07-30.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String tsvDataPathTemplate = "file://" + ConfigurationManager.getProperty("load.data.workspace") + "/work/bcp-${task}";

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
        String task = BigDataConstants.CONTENT_TYPE_FTP;
        TaskBean ftp = new TaskBean();
        ftp.setBcpPath(tsvDataPathTemplate.replace("${task}",task));
        ftp.setCaptureTimeIndex(17);
        ftp.setContentType(task);
        ftp.setDocType(BigDataConstants.SOLR_DOC_TYPE_FTP_VALUE);
        ftp.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        ftp.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        ftp.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        ftp.setHbaseTableName(NamingUtils.getHBaseTableName(task));

        return ftp;
    }

    /**
     *  聊天的任务
     * @return
     */
    private static TaskBean getImchatTask() {
        String task = BigDataConstants.CONTENT_TYPE_IM_CHAT;
        TaskBean imChat = new TaskBean();
        imChat.setBcpPath(tsvDataPathTemplate.replace("${task}",task));
        imChat.setCaptureTimeIndex(20);
        imChat.setContentType(task);
        imChat.setDocType(BigDataConstants.SOLR_DOC_TYPE_IMCHAT_VALUE);
        imChat.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        imChat.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        imChat.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        imChat.setHbaseTableName(NamingUtils.getHBaseTableName(task));

        String path = ConfigurationManager.getProperty("bcp.file.path") + File.separator + task;
        logger.info("替换 {} 的BCP数据的目录： {}", imChat.getContentType(), path);
//        imChat.replaceFileRN(path);
        return imChat;
    }

    /**
     * 网页的任务
     * @return
     */
    private static TaskBean getHttpTask() {
        String task = BigDataConstants.CONTENT_TYPE_HTTP;
        TaskBean http = new TaskBean();
        http.setBcpPath(tsvDataPathTemplate.replace("${task}",task));
        http.setCaptureTimeIndex(22);
        http.setContentType(task);
        http.setDocType(BigDataConstants.SOLR_DOC_TYPE_HTTP_VALUE);
        http.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        http.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        http.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        http.setHbaseTableName(NamingUtils.getHBaseTableName(task));


        return http;
    }

}
