package com.rainsoft.solr.distinct;

import com.rainsoft.dao.ImeiDao;
import com.rainsoft.solr.base.OracleDistinctDataExport;

/**
 * Oracle数据库 imei 数据导入Solr
 * Created by CaoWeiDong on 2018-04-24 11:22:08
 */
public class ImeiOracleDataExport {

    private static ImeiDao dao = (ImeiDao) OracleDistinctDataExport.context.getBean("imeiDao");

    private static final String task = "imei";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        OracleDistinctDataExport.extract(task, dao);
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }

}
