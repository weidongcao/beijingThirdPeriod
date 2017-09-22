package com.rainsoft.domain;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

/**
 * 任务实体(ftp/http/im_chat)
 * Created by CaoWeiDong on 2017-08-14.
 */
public class TaskBean implements Serializable {

    //BCP文件路径
    private String bcpPath;
    //HBase表名
    private String hbaseTableName;
    //HBase列簇
    private String hbaseCF;
    //HFile在HDFS上的临时存储目录
    private String hfileTmpStorePath;
    //数据类型
    private String contentType;
    //全部字段名数组
    private String[] columns;
    //关键字段名数组(为空的话需要过滤)
    private String[] keyColumns;

    public String getBcpPath() {
        return bcpPath;
    }

    public void setBcpPath(String bcpPath) {
        this.bcpPath = bcpPath;
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }

    public void setHbaseTableName(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    public String getHbaseCF() {
        return hbaseCF;
    }

    public void setHbaseCF(String hbaseCF) {
        this.hbaseCF = hbaseCF;
    }

    public String getHfileTmpStorePath() {
        return hfileTmpStorePath;
    }

    public void setHfileTmpStorePath(String hfileTmpStorePath) {
        this.hfileTmpStorePath = hfileTmpStorePath;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String[] getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(String[] keyColumns) {
        this.keyColumns = keyColumns;
    }

    @Override
    public String toString() {
        return "TaskBean{" +
                "bcpPath='" + bcpPath + '\'' +
                ", hbaseTableName='" + hbaseTableName + '\'' +
                ", hbaseCF='" + hbaseCF + '\'' +
                ", hfileTmpStorePath='" + hfileTmpStorePath + '\'' +
                ", contentType='" + contentType + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", keyColumns=" + Arrays.toString(keyColumns) +
                '}';
    }
}
