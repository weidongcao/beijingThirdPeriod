package com.rainsoft.bcp.yuntan.old1;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.domain.TaskBean;
import com.rainsoft.utils.NamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Spark处理BCP文件
 * 从Bcp文件池获取数据
 * 1.将BCP文件数据导入到Solr
 * 2.将BCP文件数据导入HBase
 * Created by CaoWeiDong on 2017-07-29.
 */
public class BcpFileImport implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(BcpFileImport.class);

    //    private static final String tsvDataPathTemplate = "file:///" + ConfigurationManager.getProperty("bcp_file_path") + File.separator;
    private static final String hbaseTablePrefix = "H_";
    private static final long serialVersionUID = 5676116812659626351L;

    public static void main(String[] args) {
        TaskBean ftp = getFtpTask();
        TaskBean im_chat = getImchatTask();
        TaskBean http = getHttpTask();
        BcpImportHBaseSolrService bcpImportHBaseSolrService = new BcpImportHBaseSolrService();
        while (true) {
            bcpImportHBaseSolrService.bcpImportHBaseSolr(ftp);
            bcpImportHBaseSolrService.bcpImportHBaseSolr(im_chat);
            bcpImportHBaseSolrService.bcpImportHBaseSolr(http);
            try {
                logger.info("一次任务处理完成休眠5秒");
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * ftp的任务
     * @return
     */
    private static TaskBean getFtpTask() {
        String task = BigDataConstants.CONTENT_TYPE_FTP;
        String oracleTableName = NamingUtils.getTableName(task);
        TaskBean ftp = new TaskBean();
        //BCP文件路径
        ftp.setBcpPath(ConfigurationManager.getProperty("bcp.file.path") + "/" + task);
        //HBase表名
        ftp.setHbaseTableName(hbaseTablePrefix + oracleTableName.toUpperCase());
        ftp.setHbaseTableName(NamingUtils.getHBaseTableName(task));
        //HBase列簇
        ftp.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        //HFile在HDFS上的临时存储目录
        ftp.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        //数据类型
        ftp.setContentType(task);
        //全部字段名数组
        ftp.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        //需要过滤的关键字段
        ftp.setKeyColumns(new String[]{"file_name"});

        logger.info("任务信息: {}", ftp.toString());
        return ftp;
    }

    /**
     *  聊天的任务
     * @return
     */
    private static TaskBean getImchatTask() {
        String task = BigDataConstants.CONTENT_TYPE_IM_CHAT;
        TaskBean imChat = new TaskBean();

        //BCP文件路径
        imChat.setBcpPath(ConfigurationManager.getProperty("bcp.file.path") + "/" + task);
        //HBase表名
        imChat.setHbaseTableName(NamingUtils.getHBaseTableName(task));
        //HBase列簇
        imChat.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        //HFile在HDFS上的临时存储目录
        imChat.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        //数据类型
        imChat.setContentType(task);
        //全部字段名数组
        imChat.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        //需要过滤的关键字段
        imChat.setKeyColumns(new String[]{});

        logger.info("任务信息: {}", imChat.toString());
        return imChat;
    }


    /**
     * 网页的任务
     * @return
     */
    private static TaskBean getHttpTask() {
        String task = BigDataConstants.CONTENT_TYPE_HTTP;
        TaskBean http = new TaskBean();

        //BCP文件路径
        http.setBcpPath(ConfigurationManager.getProperty("bcp.file.path") + "/" + task);
        //HBase表名
        http.setHbaseTableName(NamingUtils.getHBaseTableName(task));
        //HBase列簇
        http.setHbaseCF(NamingUtils.getHBaseContentTableCF());
        //HFile在HDFS上的临时存储目录
        http.setHfileTmpStorePath(NamingUtils.getHFileTaskDir(NamingUtils.getBcpTaskKey(task)));
        //数据类型
        http.setContentType(task);
        //全部字段名数组
        http.setColumns(FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingUtils.getBcpTaskKey(task)));
        //需要过滤的关键字段
        http.setKeyColumns(new String[]{"ref_domain"});

        logger.info("任务信息: {}", http.toString());
        return http;
    }
}

