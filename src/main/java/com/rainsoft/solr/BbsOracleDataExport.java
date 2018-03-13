package com.rainsoft.solr;

import com.google.common.base.Optional;
import com.rainsoft.BigDataConstants;
import com.rainsoft.dao.BbsDao;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Oracle数据库BBS数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-10-15.
 */
public class BbsOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BbsOracleDataExport.class);

    //任务类型(bbs)
    private static final String task = BigDataConstants.CONTENT_TYPE_BBS;

    private static BbsDao dao = (BbsDao) context.getBean("bbsDao");

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        //监控执行情况
        watch.start();

        Optional<Long> id = getTaskStartId(task);
        if (id.isPresent() == false) {
            //三个月前的日期
            String dateText = DateFormatUtils.DATE_FORMAT.format(DateUtils.addMonths(new Date(), -3));
            //从三个月前起的ID
            id = dao.getMinIdFromDate(Optional.of(dateText));
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
