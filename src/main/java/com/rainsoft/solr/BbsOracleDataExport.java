package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.dao.BbsDao;

import java.util.Optional;

/**
 * Oracle数据库BBS数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-10-15.
 */
public class BbsOracleDataExport extends BaseOracleDataExport {

    //任务类型(bbs)
    private static final String task = BigDataConstants.CONTENT_TYPE_BBS;

    private static BbsDao dao = (BbsDao) context.getBean("bbsDao");

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
