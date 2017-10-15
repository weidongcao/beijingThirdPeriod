package com.rainsoft;

/**
 * Created by CaoWeiDong on 2017-07-26.
 */
public class BigDataConstants {
    //oracle的rownum的别名
    public static final String ROWNUM = "rn";

    //HBase表的前缀
    public static final String HBASE_TABLE_PREFIX = "H_";

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

    //内容表模板
    public static final String TEMPLATE_ORACLE_CONTENT_TABLE = "reg_content_${type}";
    //HBase的HFile模板在HDFS上的临时存储目录模板
    public static final String TEMPLATE_HFILE_TEMP_STORE_HDFS = "/tmp/hbase/hfile/${type}";

    //HBase的列簇模板
    public static final String TEMPLATE_HBASE_CF = "CONTENT_${type}";
    //FTP在Oracle的表名
    public static final String ORACLE_TABLE_FTP_NAME = "reg_content_ftp";
    //聊天在Oracle的表名
    public static final String ORACLE_TABLE_IM_CHAT_NAME = "reg_content_im_chat";

    //HTTP在Oracle的表名
    public static final String ORACLE_TABLE_HTTP_NAME = "reg_content_http";
    //FTP在HBase的列簇
    public static final String HBASE_TABLE_FTP_CF = "CONTENT_FTP";
    //聊天表在HBase的列簇
    public static final String HBASE_TABLE_IM_CHAT_CF = "CONTENT_IM_CHAT";

    //HTTP表在HBase的列簇
    public static final String HBASE_TABLE_HTTP_CF = "CONTENT_HTTP";
    //Solr里的数据类型
    public static final String SOLR_DOC_TYPE_KEY = "docType";
    public static final String SOLR_DOC_TYPE_FTP_VALUE = "文件";
    public static final String SOLR_DOC_TYPE_HTTP_VALUE = "网页";
    public static final String SOLR_DOC_TYPE_IMCHAT_VALUE = "聊天";

    //程序生成HBase的HFile在HDFS上的临时存储目录
    public static final String TMP_HFILE_HDFS = "/tmp/hbase/hfile/";

    //内容类的表的id在Solr里的重命名（Oracle的ID，HBase的RowKey）
    public static final String SOLR_CONTENT_ID = "SID";


    //获取时间字段名
    public static final String CAPTURE_TIME = "capture_time";
}
