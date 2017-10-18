package com.rainsoft.solr;

import com.rainsoft.dao.RealDao;
import com.rainsoft.domain.RegRealIdInfo;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Oracle数据库 真实 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(RealOracleDataExport.class);

    private static RealDao dao = (RealDao) context.getBean("realDao");

    private static final String task = "real";

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
        List<String[]> dataList = dao.getRealByHours(period._1, period._2);
        logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

        //实时数据导出
        exportRealTimeData(dataList, task, period);
    }

    public static void main(String[] args){
        while (true) {
            exportOracleByTime();
        }
    }

}
