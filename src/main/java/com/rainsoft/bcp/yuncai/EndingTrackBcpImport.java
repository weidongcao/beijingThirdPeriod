package com.rainsoft.bcp.yuncai;

import com.rainsoft.FieldConstants;
import com.rainsoft.bcp.BaseBcpImportHBaseSolr;
import com.rainsoft.utils.LinuxUtils;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SparkUtils;
import com.rainsoft.utils.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * 云采
 * ending_track类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2017-12-08.
 */
public class EndingTrackBcpImport {
    public static final Logger logger = LoggerFactory.getLogger(EndingTrackBcpImport.class);

    public static final String task = "ending_trace";

    public static void runTask() {
        // 第一步:将Bcp文件从文件池移到工作目录
//        BaseBcpImportHBaseSolr.moveBcpfileToWorkDir(task, LinuxUtils.SHELL_YUNTAN_BCP_MV);

        // 第二步：从工作目录读取文件列表
        File[] files = FileUtils.getFile(
//                NamingRuleUtils.getBcpWorkDir(task)
                "D:\\0WorkSpace\\data\\yuncai"
        ).listFiles();

        if ((null != files) && (files.length > 0)) {
            for (int i = 0; i < files.length; i++) {
                List<String> list = BaseBcpImportHBaseSolr.getFileContent(files[i]);

                // 第三步: Spark读取文件内容并添加唯一主键
                JavaRDD<Row> dataRDD = SparkUtils.bcpDataAddRowkey(list, task);

                // 第四步: Spark过滤
                JavaRDD<Row> filterRDD = filterBcpData(dataRDD);

                // 第五步: RDD持久化
                filterRDD.persist(StorageLevel.MEMORY_ONLY());

                // 第六步: Spark数据导入到Solr
                BaseBcpImportHBaseSolr.bcpWriteIntoSolr(filterRDD, task);

                // 第七步: Spark数据导入到HBase
                if (BaseBcpImportHBaseSolr.isExport2HBase == true) {
                    BaseBcpImportHBaseSolr.bcpWriteIntoHBase(filterRDD, task);
                }

                // 第八步: RDD取消持久化
                filterRDD.unpersist();

                //删除文件
                files[i].delete();
            }
        } else {
            logger.info("没有需要处理的数据");
        }
    }

    public static JavaRDD<Row> filterBcpData(JavaRDD<Row> dataRDD) {

        //Bcp文件对应的字段名
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));
        //Bcp文件需要过滤的字段
        String[] filterColumns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpFilterKey(task));
        /*
         * 对关键字段进行过滤
         */
        JavaRDD<Row> filterKeyColumnRDD = dataRDD.filter(
                (Function<Row, Boolean>) values -> {
                    boolean ifKeep = true;

                    if (ArrayUtils.isNotEmpty(filterColumns)) {

                        for (String column : filterColumns) {
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
        runTask();
    }
}
