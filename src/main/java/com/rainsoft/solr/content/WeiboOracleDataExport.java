package com.rainsoft.solr.content;

import com.rainsoft.dao.WeiboDao;
import com.rainsoft.solr.base.OracleContentDataExport;

import java.util.Optional;

/**
 * Oracle数据库 微博 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class WeiboOracleDataExport {

    private static WeiboDao dao = (WeiboDao) OracleContentDataExport.context.getBean("weiboDao");

    private static final String task = "weibo";

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
