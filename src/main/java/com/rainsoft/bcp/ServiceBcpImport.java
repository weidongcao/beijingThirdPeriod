package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * service(场所)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class ServiceBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(ServiceBcpImport.class);

    //任务类型
    public static final String task = "service";
    private static final long serialVersionUID = -6383618509896386301L;

    public static void main(String[] args) throws IOException {
        while (true) {

            doTask(task);
        }
    }
}
