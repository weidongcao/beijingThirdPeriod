package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HTTP类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class HttpBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(HttpBcpImport.class);

    //任务类型
    public static final String task = "http";
    private static final long serialVersionUID = 4620835234124768948L;

    public static void main(String[] args) throws IOException {
        String os = System.getProperty("os.name");
        do {
            doTask(task);
        } while (os.toLowerCase().contains("windows") == false);
    }
}
