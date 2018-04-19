package com.rainsoft.solr.distinct;

import com.rainsoft.dao.VidDao;
import com.rainsoft.solr.base.OracleDistinctDataExport;

/**
 * Oracle数据库 虚拟 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class VidOracleDataExport {

    private static VidDao dao = (VidDao) OracleDistinctDataExport.context.getBean("vidDao");

    private static final String task = "vid";

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
