package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * real(真实)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class RealBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(RealBcpImport.class);

    //任务类型
    public static final String task = "real";
    private static final long serialVersionUID = 1380428864427522099L;

    public static void main(String[] args) {
        while (true) {
            doTask(task);
        }
    }
}
