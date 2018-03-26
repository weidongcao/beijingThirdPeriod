package com.rainsoft.solr;

import com.rainsoft.dao.SearchDao;

import java.util.Optional;

/**
 * Oracle数据库 搜索 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class SearchOracleDataExport extends BaseOracleDataExport {

    private static SearchDao dao = (SearchDao) context.getBean("searchDao");

    private static final String task = "search";

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
