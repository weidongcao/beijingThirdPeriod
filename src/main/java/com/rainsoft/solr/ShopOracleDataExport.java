package com.rainsoft.solr;

import com.rainsoft.dao.ShopDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

/**
 * Oracle数据库 shop 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(ShopOracleDataExport.class);

    private static ShopDao dao = (ShopDao) context.getBean("shopDao");

    private static final String task = "shop";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        //监控执行情况
        watch.start();

        //根据当前时间和任务类型获取要从Oracle查询的开始时间和结束时间
        Tuple2<String, String> period = getPeriod(task, 240);
        logger.info("{} : 开始索引 {} 到 {} 的数据", task, period._1, period._2);

        //获取数据库指定捕获时间段的数据
        List<String[]> dataList = dao.getShopByHours(period._1, period._2);
        logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

        //实时数据导出
        exportRealTimeData(dataList, task, period);
    }

    public static void main(String[] args) {
        while (true) {
            exportOracleByTime();
        }
    }
}

