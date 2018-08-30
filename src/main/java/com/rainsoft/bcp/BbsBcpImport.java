package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * BBS(论坛)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class BbsBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(BbsBcpImport.class);

    //任务类型
    public static final String task = "bbs";
    private static final long serialVersionUID = 8229595491228556444L;

    public static void main(String[] args) throws IOException {
        String os = System.getProperty("os.name");
        do {
            doTask(task);
        } while (os.toLowerCase().contains("windows") == false);
    }
}
