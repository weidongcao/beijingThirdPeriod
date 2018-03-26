package com.rainsoft.solr;

import com.rainsoft.dao.EmailDao;

import java.util.Optional;

/**
 * Oracle数据库BBS数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailOracleDataExport extends BaseOracleDataExport {

    //任务类型(bbs)
    private static final String task = "email";

    private static EmailDao dao = (EmailDao) context.getBean("emailDao");


    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        extract(task, dao, Optional.of(-6));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}
