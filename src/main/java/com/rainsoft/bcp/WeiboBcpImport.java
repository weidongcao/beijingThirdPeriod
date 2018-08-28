package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * weibo(微博)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class WeiboBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(WeiboBcpImport.class);

    //任务类型
    public static final String task = "weibo";
    private static final long serialVersionUID = 3342398057819847570L;

    public static void main(String[] args) throws IOException {
        while (true) {
            doTask(task);
        }
    }
}
