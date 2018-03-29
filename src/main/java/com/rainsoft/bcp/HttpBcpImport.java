package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * HTTP类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class HttpBcpImport extends BaseBcpImportHBaseSolr {
    private static final long serialVersionUID = 4620835234124768948L;
    public static final Logger logger = LoggerFactory.getLogger(HttpBcpImport.class);

    //任务类型
    public static final String task = "http";

    public static void main(String[] args) {
        while (true) {
            doTask(task);
        }
    }
}
