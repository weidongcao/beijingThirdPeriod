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
public class ImchatSparkOperateBcp extends SparkOperateBcp {
    private static final Logger logger = LoggerFactory.getLogger(ImchatSparkOperateBcp.class);

    //数据类型
    protected String contentType = BigDataConstants.CONTENT_TYPE_HTTP;
    //Sorl的docType
    protected String docType = BigDataConstants.SOLR_DOC_TYPE_IMCHAT_VALUE;
    //捕获时间在在bcp文件里一行的位置（第一个从0开始）
    protected int CaptureTimeIndexBcpFileLine = 20;

    public static void main(String[] args) {
    }
}
