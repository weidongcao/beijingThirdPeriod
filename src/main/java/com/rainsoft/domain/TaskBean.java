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
    //获取Oracle表字段名
    private String[] fields;
    //Sorl的docType
    private String docType;
    private long curTimeLong = new Date().getTime();
    //捕获时间在在bcp文件里一行的位置（第一个从0开始）
    private int CaptureTimeIndexBcpFileLine;

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

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public long getCurTimeLong() {
        return curTimeLong;
    }

    public void setCurTimeLong(long curTimeLong) {
        this.curTimeLong = curTimeLong;
    }

    public int getCaptureTimeIndexBcpFileLine() {
        return CaptureTimeIndexBcpFileLine;
    }

    public void setCaptureTimeIndexBcpFileLine(int captureTimeIndexBcpFileLine) {
        CaptureTimeIndexBcpFileLine = captureTimeIndexBcpFileLine;
    }

    @Override
    public String toString() {
        return "TaskBean{" +
                "bcpPath='" + bcpPath + '\'' +
                ", hbaseTableName='" + hbaseTableName + '\'' +
                ", hbaseCF='" + hbaseCF + '\'' +
                ", hfileTmpStorePath='" + hfileTmpStorePath + '\'' +
                ", contentType='" + contentType + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", docType='" + docType + '\'' +
                ", curTimeLong=" + curTimeLong +
                ", CaptureTimeIndexBcpFileLine=" + CaptureTimeIndexBcpFileLine +
                '}';
    }
}
