package com.rainsoft.solr;

import com.rainsoft.dao.WeiboDao;

import java.util.Optional;

/**
 * Oracle数据库 微博 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class WeiboOracleDataExport extends BaseOracleDataExport {

    private static WeiboDao dao = (WeiboDao) context.getBean("weiboDao");

    private static final String task = "weibo";

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
