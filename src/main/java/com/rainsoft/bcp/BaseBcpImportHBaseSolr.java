package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SolrUtil;
import com.rainsoft.utils.io.FileUtils;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Bcp文件导入Hbase，Solr
 * Created by CaoWeiDong on 2017-09-29.
 */
class BaseBcpImportHBaseSolr implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BaseBcpImportHBaseSolr.class);

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    //创建Solr客户端
    protected static SolrClient client = (SolrClient) context.getBean("solrClient");
    private static JavaSparkContext sparkContext = null;

    //将Bcp文件从文件池中移到工作目录命令模板
    private static final String shellMvTemplate = "find ${bcp_pool_dir} -name \"*-${task}*.bcp\"  | tail -n ${operator_bcp_number} |xargs -i mv {} ${bcp_file_path}/${task}";
    //Bcp文件池目录
    private static final String bcpPoolDir = ConfigurationManager.getProperty("bcp.receive.dir");
    //Bcp文件移动的个数
    private static final String operatorBcpNumber = ConfigurationManager.getProperty("operator.bcp.number");
    //要移动到的目录
    private static final String bcpFilePath = ConfigurationManager.getProperty("bcp.file.path");
    //是否导入到HBase
    private static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");

    /**
     * 获取SparkContext
     *
     * @return SparkContext
     */
    private static JavaSparkContext getSparkContext() {
        if (sparkContext == null || sparkContext.env().isStopped()) {
            SparkConf conf = new SparkConf()
                    .setAppName(BcpFileImport.class.getSimpleName())
                    .set("spark.ui.port", "4050")
                    .setMaster("local");
            sparkContext = new JavaSparkContext(conf);
        }

        return sparkContext;

    }

    /**
     * 将Bcp文件的内容导入到HBase、Solr
     *
     * @param task 任务类型
     */
    static void filesContentImportHBaseSolr(String task) {
        //BCP文件所在目录
        File dir = FileUtils.getFile(NamingRuleUtils.getBcpWorkDir(task));
        //BCP文件列表
        File[] files = dir.listFiles();

        //遍历BCP文件,数据导入Solr、HBase
        assert files != null;
        for (File file : files) {
            List<String> lines;
            try {
                lines = FileUtils.readLines(file);
                runImport(lines, task);
                //删除文件
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("读取文件失败：file={}, error={}", file.getAbsolutePath(), e);
                //执行失败把此文件移动出去
                file.renameTo(new File("/opt/bcp/error", file.getName()));
                System.exit(-1);
            }
        }
    }

    /**
     * 开始导入任务
     *
     * @param lines Bcp 数据
     * @param task  任务类型
     */
    private static void runImport(List<String> lines, String task) {
        JavaRDD<String> originalRDD = getSparkContext().parallelize(lines);

        //Bcp文件对应的字段名
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));
        //Bcp文件需要过滤的字段
        String[] filterColumns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpFilterKey(task));
        //获取时间在字段名数组的下标
        int captureTimeIndex = ArrayUtils.indexOf(columns, BigDataConstants.CAPTURE_TIME);

        /*
         * 对BCP文件数据进行基本的处理
         * 生成ID(HBase的RowKey，Solr的Sid)
         * 将ID作为在第一列
         *
         */
        JavaRDD<String[]> valueArrayRDD = originalRDD.mapPartitions(
                (FlatMapFunction<Iterator<String>, String[]>) iter -> {
                    List<String[]> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        String str = iter.next();
                        //字段值数组
                        String[] fields = str.split(BigDataConstants.BCP_FIELD_SEPARATOR);
                        //生成 rowkey&id
                        String rowKey = createRowKey(fields, captureTimeIndex);
                        //rowkey添加到数组
                        fields = ArrayUtils.add(fields, 0, rowKey);
                        //添加到值列表
                        list.add(fields);
                    }
                    return list.iterator();
                }
        );

        /*
         * 对关键字段进行过滤
         */
        JavaRDD<String[]> filterKeyColumnRDD = valueArrayRDD.filter(
                (Function<String[], Boolean>) values -> {
                    boolean ifFilter = true;

                    if (ArrayUtils.isNotEmpty(filterColumns)) {

                        for (String column : filterColumns) {
                            int index = ArrayUtils.indexOf(columns, column.toLowerCase());
                            if (StringUtils.isBlank(values[index + 1])) {
                                ifFilter = false;
                                break;
                            }
                        }
                    }
                    return ifFilter;
                }
        );
        //RDD持久化
        filterKeyColumnRDD.persist(StorageLevel.MEMORY_ONLY());
        //Bcp文件数据写入Solr
        bcpWriteIntoSolr(filterKeyColumnRDD, task);

        //BCP文件数据写入HBase
        if (isExport2HBase) {
            bcpWriteIntoHBase(filterKeyColumnRDD, task);
        }
    }

    /**
     * Bcp数据导入到Solr
     *
     * @param javaRDD JavaRDD
     * @param task    任务类型
     */
    private static void bcpWriteIntoSolr(JavaRDD<String[]> javaRDD, String task) {
        logger.info("开始将 {} 的BCP数据索引到Solr", task);
        //Bcp文件对应的字段名
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));

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
                        //SID
                        doc.addField(BigDataConstants.SOLR_CONTENT_ID.toUpperCase(), rowkey);

                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));

                        if (rowkey.contains("_")) {
                            //ID
                            doc.addField("ID", rowkey.split("_")[1]);
                            //capture_time
                            doc.addField(BigDataConstants.CAPTURE_TIME, rowkey.split("_")[0]);
                        } else {
                            //ID
                            doc.addField("ID", rowkey);
                        }

                        //import_time
                        Date curDate = new Date();
                        doc.addField("import_time".toUpperCase(), DateFormatUtils.DATE_TIME_FORMAT.format(curDate));
                        doc.addField("import_time".toLowerCase(), curDate.getTime());

                        for (int i = 1; i < str.length; i++) {
                            String value = str[i];
                            if ((i - 1) >= columns.length) {
                                break;
                            }
                            String key = columns[i - 1].toUpperCase();
                            //如果字段的值为空则不写入Solr
                            if (StringUtils.isNotBlank(value)) {
                                doc.addField(key, value);
                            }
                        }
                        list.add(doc);

                    }
                    if (list.size() > 0) {
                        //写入Solr
                        client.add(list, 1000);
                        logger.info("---->写入Solr {} 条数据成功", list.size());
                    } else {
                        logger.info("{} 此Spark Partition 数据为空", task);
                    }
                }
        );

        logger.info("####### {}的BCP数据索引Solr完成 #######", task);
    }

    /**
     * Bcp数据导入到HBase
     *
     * @param javaRDD JavaRDD
     * @param task    任务类型
     */
    private static void bcpWriteIntoHBase(JavaRDD<String[]> javaRDD, String task) {
        logger.info("{}的BCP数据开始写入HBase...", task);

        //将要作为rowkey的字段，service_info表是service_code，其他表都是id
        String rowkeyColumn;
        if (task.equalsIgnoreCase("service")) {
            rowkeyColumn = "service_code";
        } else {
            rowkeyColumn = "id";
        }
        //将数据转为可以进行二次排序的形式
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(
                javaRDD,
                ArrayUtils.add(
                        FieldConstants.COLUMN_MAP.get(
                                NamingRuleUtils.getBcpTaskKey(task)
                        ),
                        0,
                        rowkeyColumn
                ),
                rowkeyColumn
        );

        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);

        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    /**
     * Java执行外部Shell命令
     *
     * @param task    任务类型
     * @param shellMv 要执行的Shell命令
     */
    private static void execShell(String task, String shellMv) {
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
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("执行 Shell命令失败:{}", shellMv);
            System.exit(-1);
        }
    }

    /**
     * 将Bcp文件从接收目录移动到工作目录
     *
     * @param task 任务类型
     */
    static void moveBcpfileToWorkDir(String task) {

        String shellMv = shellMvTemplate.replace("${bcp_pool_dir}", bcpPoolDir)
                .replace("${task}", task)
                .replace("${operator_bcp_number}", operatorBcpNumber)
                .replace("${bcp_file_path}", bcpFilePath);

        execShell(task, shellMv);
    }

    /**
     * 生成HBase的RowKey&Solr的ID
     *
     * @param fields           字段名数组
     * @param captureTimeIndex 获取时间下标
     * @return rowkey
     */
    private static String createRowKey(String[] fields, int captureTimeIndex) {
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
                e.printStackTrace();
                rowKey = null;
            }
        } else {
            rowKey = uuid;
        }
        return rowKey;
    }

    public static void main(String[] args) {

    }
}


