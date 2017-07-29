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

    //FTP在Oracle的表名
    public static final String ORACLE_TABLE_FTP_NAME = "reg_content_ftp";

    //FTP在HBase的列簇
    public static final String HBASE_TABLE_FTP_CF = "CONTENT_FTP";

    //Solr里的数据类型
    public static final String SOLR_DOC_TYPE_KEY = "docType";
    public static final String SOLR_DOC_TYPE_FTP_VALUE = "文件";
    public static final String SOLR_DOC_TYPE_HTTP_VALUE = "网页";
    public static final String SOLR_DOC_TYPE_IMCHAT_VALUE = "聊天";

    //内容类的表的id在Solr里的重命名（Oracle的ID，HBase的RowKey）
    public static final String SOLR_CONTENT_ID = "sid";


}
