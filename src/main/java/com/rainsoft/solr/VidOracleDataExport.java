package com.rainsoft.solr;

import com.google.common.base.Optional;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.dao.VidDao;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

/**
 * Oracle数据库 虚拟 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class VidOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(VidOracleDataExport.class);

    private static VidDao dao = (VidDao) context.getBean("vidDao");

    private static final String task = "vid";

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
