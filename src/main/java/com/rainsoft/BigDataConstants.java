package com.rainsoft;

/**
 * Created by CaoWeiDong on 2017-07-26.
 */
public class BigDataConstants {
    //oracle的rownum的别名
    public static final String ROWNUM = "rn";

    //BCP文件字段分隔符
    public static final String BCP_FIELD_SEPARATOR = "\\|#\\|";

    //BCP文件数据数据列分隔符
    public static final String BCP_LINE_SEPARATOR = "\\|\\$\\|";

    //内容表数据类型
    public static final String CONTENT_TYPE_FTP = "ftp";
    public static final String CONTENT_TYPE_HTTP = "http";
    public static final String CONTENT_TYPE_IM_CHAT = "im_chat";
    public static final String CONTENT_TYPE_BBS = "bbs";
    public static final String CONTENT_TYPE_EMAIL = "email";
    public static final String CONTENT_TYPE_REAL = "real";
    public static final String CONTENT_TYPE_SEARCH = "search";
    public static final String CONTENT_TYPE_SERVICE = "service";
    public static final String CONTENT_TYPE_SHOP = "shop";
    public static final String CONTENT_TYPE_VID = "vid";
    public static final String CONTENT_TYPE_WEIBO = "weibo";

    //Solr里的数据类型
    public static final String SOLR_DOC_TYPE_KEY = "docType";
    public static final String SOLR_DOC_TYPE_FTP_VALUE = "文件";
    public static final String SOLR_DOC_TYPE_HTTP_VALUE = "网页";
    public static final String SOLR_DOC_TYPE_IMCHAT_VALUE = "聊天";

    //内容类的表的id在Solr里的重命名（Oracle的ID，HBase的RowKey）
    public static final String SOLR_CONTENT_ID = "SID";


    //获取时间字段名
    public static final String CAPTURE_TIME = "capture_time";
}
