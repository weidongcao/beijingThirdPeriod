package com.rainsoft.solr;

import com.rainsoft.dao.RealDao;

import java.util.Optional;

/**
 * Oracle数据库 真实 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealOracleDataExport extends BaseOracleDataExport {

    private static RealDao dao = (RealDao) context.getBean("realDao");

    private static final String task = "real";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        extract(task, dao, Optional.empty());
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }

}
