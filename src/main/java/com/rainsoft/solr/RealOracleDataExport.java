package com.rainsoft.solr;

import com.google.common.base.Optional;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.dao.RealDao;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Date;
import java.util.List;

/**
 * Oracle数据库 真实 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(RealOracleDataExport.class);

    private static RealDao dao = (RealDao) context.getBean("realDao");

    private static final String task = "real";

    //sync time
    private static int hours = ConfigurationManager.getInteger("oracle.batch.export.length");

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        //监控执行情况
        watch.start();

        Optional<Long> id = getTaskStartId(task);
        if (id.isPresent() == false) {
            id = dao.getMinId();
            //添加到输入记录Map
            recordMap.put(NamingRuleUtils.getOracleRecordKey(task), id.get());
        }
        //获取数据库指定捕获时间段的数据
        List<String[]> dataList = dao.getDataById(id);

        //实时数据导出
        exportRealTimeData(dataList, task);
    }

    public static void main(String[] args) {
        while (true) {
            exportOracleByTime();
        }
    }

}
