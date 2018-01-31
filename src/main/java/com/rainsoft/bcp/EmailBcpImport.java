package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * email(邮件)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class EmailBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(EmailBcpImport.class);

    //任务类型
    public static final String task = "email";
    private static final long serialVersionUID = -5388049810271686103L;

    public static void main(String[] args) {
        while (true) {
            doTask(task);
        }
    }
}
