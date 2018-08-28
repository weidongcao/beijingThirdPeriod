package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * FTP(文件)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class FtpBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(FtpBcpImport.class);

    //任务类型
    public static final String task = "ftp";
    private static final long serialVersionUID = 2166260684440343318L;

    public static void main(String[] args) throws IOException {
        while (true) {
            doTask(task);
        }
    }
}
