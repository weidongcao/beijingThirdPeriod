package com.rainsoft.spark;

import com.rainsoft.BigDataConstants;
import com.rainsoft.bcp.yuntan.old2.SparkOperateBcp;

/**
 * Spark处理BCP文件
 * 主要进行两项操作
 * 1.将BCP文件数据导入到Solr
 * 2.将BCP文件数据导入HBase
 * Created by CaoWeiDong on 2017-07-29.
 */
public class ImchatSparkOperateBcp extends SparkOperateBcp {


    private static final long serialVersionUID = -3286917479275863075L;

    //数据类型
    protected String contentType = BigDataConstants.CONTENT_TYPE_HTTP;
    //Sorl的docType
    protected String docType = BigDataConstants.SOLR_DOC_TYPE_IMCHAT_VALUE;
    //捕获时间在在bcp文件里一行的位置（第一个从0开始）
    protected int CaptureTimeIndexBcpFileLine = 20;

    public static void main(String[] args) {
    }
}
