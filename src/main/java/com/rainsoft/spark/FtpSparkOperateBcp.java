package com.rainsoft.spark;

import com.rainsoft.bcp.yuntan.old2.SparkOperateBcp;
import com.rainsoft.BigDataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark处理BCP文件
 * 主要进行两项操作
 * 1.将BCP文件数据导入到Solr
 * 2.将BCP文件数据导入HBase
 * Created by CaoWeiDong on 2017-07-29.
 */
public class FtpSparkOperateBcp extends SparkOperateBcp {
    private static final Logger logger = LoggerFactory.getLogger(FtpSparkOperateBcp.class);

    //数据类型
    protected String contentType = BigDataConstants.CONTENT_TYPE_FTP;
    //Sorl的docType
    protected String docType = BigDataConstants.SOLR_DOC_TYPE_FTP_VALUE;

    public static void main(String[] args) {
        FtpSparkOperateBcp ftp = new FtpSparkOperateBcp();
        logger.info("开始实时从BCP文件导入{}数据到Solr和HBase", ftp.contentType.toUpperCase());
        logger.info("实时从BCP文件导入{}数据到Solr和HBase结束", ftp.contentType.toUpperCase());
    }
}
