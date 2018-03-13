package com.rainsoft.solr;

import com.google.common.base.Optional;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.dao.ServiceDao;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Oracle数据库 场所 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ServiceOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(ServiceOracleDataExport.class);

    private static ServiceDao dao = (ServiceDao) context.getBean("serviceDao");

    private static final String task = "service";

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
