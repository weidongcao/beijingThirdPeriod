package com.rainsoft.solr.content;

import com.rainsoft.dao.ShopDao;
import com.rainsoft.solr.base.OracleContentDataExport;

import java.util.Optional;

/**
 * Oracle数据库 shop 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopOracleDataExport {

    private static ShopDao dao = (ShopDao) OracleContentDataExport.context.getBean("shopDao");

    private static final String task = "shop";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        OracleContentDataExport.extract(task, dao, Optional.of(-3));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}

