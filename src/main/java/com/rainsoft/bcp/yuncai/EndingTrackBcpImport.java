package com.rainsoft.bcp.yuncai;

import com.rainsoft.FieldConstants;
import com.rainsoft.bcp.BaseBcpImportHBaseSolr;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SparkUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * 云采
 * ending_track类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2017-12-08.
 */
public class EndingTrackBcpImport extends BaseBcpImportHBaseSolr {

    private static final long serialVersionUID = 469839933566133855L;
    public static final Logger logger = LoggerFactory.getLogger(EndingTrackBcpImport.class);

    public static final String task = "ending_trace";

    public static void runTask() {
        //第一步: 获取BCP文件列表
        File[] bcpFiles = getTaskBcpFiles(task);

        if ((null != bcpFiles) && (bcpFiles.length > 0)) {
            for (int i = 0; i < bcpFiles.length; i++) {
                //将BCP文件读出为List<String>
                List<String> list = getFileContent(bcpFiles[i]);
                // 第三步: Spark读取文件内容并添加唯一主键
                JavaRDD<Row> dataRDD = SparkUtils.bcpDataAddRowkey(list, task);
                // 第四步: Spark过滤
                JavaRDD<Row> filterRDD = filterBcpData(dataRDD);
                // 第五步: 写入到Sorl、HBase
                bcpWriteIntoHBaseSolr(filterRDD, task);
                // 第六步: 删除文件
                bcpFiles[i].delete();
            }
        } else {
            logger.info("{} 没有需要处理的数据", task);
        }
    }

    /**
     * 对Bcp文件的关键字段进行过滤,
     * 过滤字段为空或者格式不对什么的
     *
     * @param dataRDD JavaRDD<Row>
     * @return JavaRDD<Row>
     */
    public static JavaRDD<Row> filterBcpData(JavaRDD<Row> dataRDD) {
        //Bcp文件对应的字段名
        String[] columns = FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));
        //Bcp文件需要检验的字段
        Set<String> checkColumns = FieldConstants.FILTER_COLUMN_MAP.get(NamingRuleUtils.getBcpFilterKey(task));
        /*
         * 对关键字段进行过滤
         */
        JavaRDD<Row> filterKeyColumnRDD = dataRDD.filter(
                (Function<Row, Boolean>) values -> {
                    boolean ifKeep = true;
                    if (checkColumns.size() > 0) {
                        for (String column : checkColumns) {
                            int index = ArrayUtils.indexOf(columns, column.toLowerCase());
                            //字段值比字段名多了一列rowkey
                            if (StringUtils.isBlank(values.getString(index + 1))) {
                                ifKeep = false;
                                break;
                            }
                        }
                    }
                    return ifKeep;
                }
        );

        return filterKeyColumnRDD;
    }

    public static void main(String[] args) {
        while (true) {
            runTask();
        }
    }
}
