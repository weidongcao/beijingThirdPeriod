package com.rainsoft.solr.content;

import com.rainsoft.dao.SearchDao;
import com.rainsoft.solr.base.OracleContentDataExport;

import java.util.Optional;

/**
 * Oracle数据库 搜索 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class SearchOracleDataExport  {

    private static SearchDao dao = (SearchDao) OracleContentDataExport.context.getBean("searchDao");

    private static final String task = "search";

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
