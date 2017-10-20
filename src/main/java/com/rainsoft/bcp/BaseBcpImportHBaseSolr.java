package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.domain.TaskBean;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Bcp文件导入Hbase，Solr
 * Created by CaoWeiDong on 2017-09-29.
 */
public class BaseBcpImportHBaseSolr implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BaseBcpImportHBaseSolr.class);
    static SolrClient client = SolrUtil.getClusterSolrClient();
    static JavaSparkContext sparkContext = null;
    //将Bcp文件从文件池中移到工作目录命令模板
    static final String shellMvTemplate = "find ${bcp_pool_dir} -name \"*-${task}*.bcp\"  | tail -n ${operator_bcp_number} |xargs -i mv {} ${bcp_file_path}/${task}";
    //Bcp文件池目录
    static final String bcpPoolDir = ConfigurationManager.getProperty("bcp.receive.dir");
    //Bcp文件移动的个数
    static final String operatorBcpNumber = ConfigurationManager.getProperty("operator.bcp.number");
    //要移动到的目录
    static final String bcpFilePath = ConfigurationManager.getProperty("bcp.file.path");

    public JavaSparkContext getSparkContext() {
        if (sparkContext == null || sparkContext.env().isStopped()) {
            SparkConf conf = new SparkConf()
                    .setAppName(BcpFileImport.class.getSimpleName())
                    .set("spark.ui.port", "4050")
                    .setMaster("local");
            sparkContext = new JavaSparkContext(conf);
        }

        return sparkContext;

    }

    public void runImport(List<String> lines, String task) {
        JavaRDD<String> originalRDD = getSparkContext().parallelize(lines);

        //Bcp文件对应的字段名
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));
        //获取时间在字段名数组的下标
        int captureTimeIndex = ArrayUtils.indexOf(columns, BigDataConstants.CAPTURE_TIME);

        //对BCP文件数据进行基本的处理，并生成ID(HBase的RowKey，Solr的Sid)
        JavaRDD<String[]> valueArrayRDD = originalRDD.mapPartitions(
                (FlatMapFunction<Iterator<String>, String[]>) iter -> {
                    List<String[]> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        String str = iter.next();
                        String[] fields = str.split(BigDataConstants.BCP_FIELD_SEPARATOR);

                        //生成rowkey
                        String rowKey;
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        if (captureTimeIndex > 0) {

                            //捕获时间的毫秒，HBase按毫秒将同一时间捕获的数据聚焦到一起
                            long captureTimeMinSecond;
                            try {
                                captureTimeMinSecond = DateFormatUtils.DATE_TIME_FORMAT.parse(fields[captureTimeIndex]).getTime();
                                //捕获时间的毫秒+UUID作为数据的ID(HBase的rowKey,Solr的SID, Oracle的ID)
                                rowKey = captureTimeMinSecond + "_" + uuid;
                            } catch (Exception e) {
                                continue;
                            }
                        } else {
                            rowKey = uuid;
                        }

                        fields = ArrayUtils.add(fields, 0, rowKey);
                        list.add(fields);
                    }
                    return list;
                }
        );

        JavaRDD<String[]> filterKeyColumnRDD = valueArrayRDD.filter(
                new Function<String[], Boolean>() {
                    @Override
                    public Boolean call(String[] strings) throws Exception {
                        return validColumns(strings, task);
                    }
                }
        );
        filterKeyColumnRDD.persist(StorageLevel.MEMORY_ONLY());
        if (logger.isDebugEnabled()) {
            List<String[]> testList = filterKeyColumnRDD.take(5);
            for (String[] arr :
                    testList) {
                logger.debug("{}", StringUtils.join(arr, "\t\t"));
            }
        }
        //Bcp文件数据写入Solr
//        bcpWriteIntoSolr(filterKeyColumnRDD, task);

        //BCP文件数据写入HBase
//        bcpWriteIntoHBase(filterKeyColumnRDD, task);


    }

    public static void bcpWriteIntoSolr(JavaRDD<String[]> javaRDD, TaskBean task) {
        logger.info("开始将 {} 的BCP数据索引到Solr", task.getContentType());

        /*
         * 数据写入Solr
         */
        javaRDD.foreachPartition(
                (VoidFunction<Iterator<String[]>>) iterator -> {
                    List<SolrInputDocument> list = new ArrayList<>();
                    while (iterator.hasNext()) {
                        //数据列数组
                        String[] str = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        String rowkey = str[0];
                        //ID
                        doc.addField("ID", rowkey.split("_")[1]);

                        //SID
                        doc.addField(BigDataConstants.SOLR_CONTENT_ID.toUpperCase(), rowkey);

                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task.getContentType()));

                        //capture_time
                        doc.addField(BigDataConstants.CAPTURE_TIME, rowkey.split("_")[0]);

                        //import_time
                        Date curDate = new Date();
                        doc.addField("import_time".toUpperCase(), DateFormatUtils.DATE_TIME_FORMAT.format(curDate));
                        doc.addField("import_time".toLowerCase(), curDate.getTime());

//                        String[] values = ArrayUtils.subarray(str, 1, str.length);
                        for (int i = 1; i < str.length; i++) {
                            String value = str[i];
                            if (task.getColumns().length <= i - 1) {
                                break;
                            }
                            String key = task.getColumns()[i - 1].toUpperCase();
                            //如果字段的值为空则不写入Solr
                            if ((null != value) && (!"".equals(value))) {
                                doc.addField(key, value);
                            }
                        }
                        list.add(doc);

                    }
                    if (list.size() > 0) {
                        //写入Solr
                        client.add(list, 1000);
                        logger.info("---->写入Solr成功数据量:{}", list.size());
                    } else {
                        logger.info("{} 此Spark Partition 数据为空", task.getContentType());
                    }
                }
        );

        logger.info("####### {}的BCP数据索引Solr完成 #######", task.getContentType());
    }

    public static void bcpWriteIntoHBase(JavaRDD<String[]> javaRDD, TaskBean task) {
        logger.info("{}的BCP数据开始写入HBase...", task.getContentType());

        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = javaRDD.flatMapToPair(
                (PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>) strings -> {
                    List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                    //获取HBase的RowKey
                    String rowKey = strings[0];
                    //将一条数据转为HBase能够识别的形式
                    HBaseUtils.addFields(strings, task, list, rowKey);
                    return list;
                }
        ).sortByKey();
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task.getHbaseTableName(), task.getHbaseCF(), task.getHfileTmpStorePath());
        logger.info("####### {}的BCP数据写入HBase完成 #######", task.getContentType());
    }

    /**
     * 检测数据关键字段是否有为空
     *
     * @param fieldValues 要检测的数组
     * @param task        要检测的任务
     * @return 是否过滤(false: 不过滤, true: 过滤)
     */
    public static boolean validColumns(String[] fieldValues, String task) {
        boolean ifFilter = true;
//        for (String column :
//                task.getKeyColumns()) {
//            int index = ArrayUtils.indexOf(task.getColumns(), column.toLowerCase());
//            if (StringUtils.isBlank(fieldValues[index + 1])) {
//                ifFilter = false;
//                break;
//            }
//        }
        return ifFilter;
    }
    public static void execShell(String task, String shellMv) {
        //执行Shell命令,将Bcp文件从文件池移动到工作目录
        try {
            logger.info("执行Shell命令,将 {} 类型的Bcp文件从文件池移动到工作目录", task);
            logger.info("执行 Shell命令:{}", shellMv);
            Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", shellMv});
            int exitVal = p.waitFor();
            if (exitVal != 0) {
                BufferedInputStream in = new BufferedInputStream(p.getErrorStream());
                logger.error("执行 {} 失败:{}", shellMv, IOUtils.toString(in, "utf-8"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("执行 Shell命令失败:{}", shellMv);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void moveBcpfileToWorkDir(String task) {

        String shellMv = shellMvTemplate.replace("${bcp_pool_dir}", bcpPoolDir)
                .replace("${task}", task)
                .replace("${operator_bcp_number}", operatorBcpNumber)
                .replace("${bcp_file_path}", bcpFilePath);

        execShell(task, shellMv);
    }
}


