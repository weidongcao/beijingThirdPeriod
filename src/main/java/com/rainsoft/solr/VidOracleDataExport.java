package com.rainsoft.solr;

import com.rainsoft.dao.VidDao;

import java.util.Optional;

/**
 * Oracle数据库 虚拟 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class VidOracleDataExport extends BaseOracleDataExport {

    private static VidDao dao = (VidDao) context.getBean("vidDao");

    private static final String task = "vid";

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
