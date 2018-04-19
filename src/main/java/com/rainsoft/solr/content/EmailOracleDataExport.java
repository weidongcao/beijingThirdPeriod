package com.rainsoft.solr.content;

import com.rainsoft.dao.EmailDao;
import com.rainsoft.solr.base.OracleContentDataExport;

import java.util.Optional;

/**
 * Oracle数据库BBS数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailOracleDataExport {

    //任务类型(bbs)
    private static final String task = "email";

    private static EmailDao dao = (EmailDao) OracleContentDataExport.context.getBean("emailDao");


    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        OracleContentDataExport.extract(task, dao, Optional.of(-6));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}
