package com.rainsoft.solr;

import com.rainsoft.dao.ShopDao;

import java.util.Optional;

/**
 * Oracle数据库 shop 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopOracleDataExport extends BaseOracleDataExport {

    private static ShopDao dao = (ShopDao) context.getBean("shopDao");

    private static final String task = "shop";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        extract(task, dao, Optional.of(-3));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}

