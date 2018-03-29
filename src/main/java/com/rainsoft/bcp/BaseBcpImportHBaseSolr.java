package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.*;
import com.rainsoft.utils.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Bcp文件导入Hbase，Solr
 * <p>
 * Created by CaoWeiDong on 2017-09-29.
 */
public class BaseBcpImportHBaseSolr implements Serializable {

    public static final Logger logger = LoggerFactory.getLogger(BaseBcpImportHBaseSolr.class);
    private static final long serialVersionUID = -7320556147408518899L;

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    //创建Solr客户端
    protected static SolrClient client = (SolrClient) context.getBean("solrClient");

    //Bcp文件池目录
    private static final String bcpPoolDir = ConfigurationManager.getProperty("bcp.receive.dir");
    //Bcp文件移动的个数
    private static final String operatorBcpNumber = ConfigurationManager.getProperty("operator.bcp.number");
    //要移动到的目录
    private static final String bcpFilePath = ConfigurationManager.getProperty("bcp.file.path");
    //是否导入到HBase
    private static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkSession
    protected static SparkSession spark = new SparkSession.Builder()
            .appName(BaseBcpImportHBaseSolr.class.getSimpleName())
            .master("local")
            .getOrCreate();

    /**
     * 执行任务
     * 实现以下逻辑：
     * 1. 获取BCP文件列表
     *      获取时做以下事情：
     *          1.调用Linux命令将BCP文件从文件池移动到工作目录
     *          2. 得到返回的文件列表
     * 2. 将文件数据按BCP文件拆分成List
     * 3. 读到Spark并给给数据添加唯一主键
     * 4. 过滤关键字段
     * 5. 写入到Solr及HBase
     * 6. 删除BCP文件
     */
    static String doTask(String task) {
        //第一步: 获取BCP文件列表
        File[] bcpFiles = getTaskBcpFiles(task);

        if ((null != bcpFiles) && (bcpFiles.length > 0)) {
            for (File bcpFile : bcpFiles) {
                // 第二步：将BCP文件读出为List<String>
                List<String> list = getFileContent(bcpFile);

                // 第三步: Spark读取文件内容并添加唯一主键
                JavaRDD<String> originalRDD = SparkUtils.getSparkContext(spark).parallelize(list);
                JavaRDD<Row> dataRDD = SparkUtils.bcpDataAddRowkey(originalRDD, task);

                // 第四步: Spark过滤
                JavaRDD<Row> filterRDD = filterBcpData(dataRDD, task);

                // 第五步: 写入到Sorl、HBase
                bcpWriteIntoHBaseSolr(filterRDD, task);

                // 第六步: 删除文件
                bcpFile.delete();
            }
            return "success";
        } else {
            logger.info("{} 没有需要处理的数据", task);
            return "none";
        }
    }

    /**
     * 将Bcp文件的内容导入到HBase、Solr
     * @param file Bcp文件
     * @return List<String
     */
    private static List<String> getFileContent(File file) {
        List<String> lines = null;
        try {
            lines = FileUtils.readLines(file);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("读取文件失败：file={}, error={}", file.getAbsolutePath(), e);
            //执行失败把此文件移动出去
            file.renameTo(new File("/opt/bcp/error", file.getName()));
            System.exit(-1);
        }
        return lines;
    }

    /**
     * 根据任务类型将BCP文件从数据池移动到工作目录
     * 并返回文件列表
     * @param task 任务类型
     * @return 文件列表
     */
    private static File[] getTaskBcpFiles(String task) {
        //判断操作系统类型，Window上做测试，Linux上做生产
        String os = System.getProperty("os.name");
        File[] files;
        //适应在Windows上测试与Linux运行
        if (os.toLowerCase().contains("windows")) {     //Windows操作系统
            // 第二步：从工作目录读取文件列表
            files = FileUtils.getFile("D:\\0WorkSpace\\Develop\\data\\bcp").listFiles();
        } else {    //Linux操作系统。
            // 第一步:将Bcp文件从文件池移到工作目录
            moveBcpfileToWorkDir(LinuxUtils.SHELL_YUNTAN_BCP_MV, task);
            // 第二步：从工作目录读取文件列表
            files = FileUtils.getFile(NamingRuleUtils.getBcpWorkDir(task)).listFiles();
        }
        return files;
    }
    /**
     * 数据写入Solr、HBase
     * @param dataRDD 要写入的数据
     * @param task   任务类型
     */
    private static void bcpWriteIntoHBaseSolr(JavaRDD<Row> dataRDD, String task) {
        // RDD持久化
        dataRDD.persist(StorageLevel.MEMORY_ONLY());
        // Spark数据导入到Solr
        bcpWriteIntoSolr(dataRDD, task);
        // Spark数据导入到HBase
        if (isExport2HBase) {
            bcpWriteIntoHBase(dataRDD, task);
        }
        // RDD取消持久化
        dataRDD.unpersist();
    }

    /**
     * Bcp数据导入到Solr
     * @param javaRDD JavaRDD
     * @param task    任务类型
     */
    private static void bcpWriteIntoSolr(JavaRDD<Row> javaRDD, String task) {
        logger.info("开始将 {} 的BCP数据索引到Solr", task);
        //Bcp文件对应的字段名
        String[] columns = FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));

        // 数据写入Solr
        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> list = new ArrayList<>();
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        //SolrImputDocument添加通用外部字段
                        SolrInputDocument doc = addSolrExtraField(task, row);
                        for (int i = 0; i < row.length(); i++) {
                            if (i >= columns.length) {
                                break;
                            }
                            String value = row.getString(i + 1);
                            String key = columns[i].toUpperCase();
                            //添加字段到SolrInputDocument
                            SolrUtil.addSolrFieldValue(doc, key, value);
                        }
                        list.add(doc);
                    }
                    //写入Solr
                    SolrUtil.submitToSolr(client, list, 0, new Date());
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
    private static void bcpWriteIntoHBase(JavaRDD<Row> javaRDD, String task) {
        logger.info("{}的BCP数据开始写入HBase...", task);

        //将数据转为可以进行二次排序的形式
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(
                javaRDD,
                FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task))
        );

        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);

        logger.info("Oracle {} 数据写入HBase完成", task);
    }


    /**
     * 将Bcp文件从接收目录移动到工作目录
     *
     * @param task 任务类型
     */
    private static void moveBcpfileToWorkDir(String shellMvTemplate, String task) {

        String shellMv = shellMvTemplate.replace("${bcp_pool_dir}", bcpPoolDir)
                .replace("${task}", task)
                .replace("${operator_bcp_number}", operatorBcpNumber)
                .replace("${bcp_file_path}", bcpFilePath);

        LinuxUtils.execShell(shellMv, task);
    }

    /**
     * 生成SolrInputDocument并添加通用的额外字段
     *
     * @param task 任务类型
     * @param row  row
     * @return SolrInputDocument
     */
    private static SolrInputDocument addSolrExtraField(String task, Row row) {
        SolrInputDocument doc = new SolrInputDocument();
        String rowkey = row.getString(0);
        //SID
        doc.addField(BigDataConstants.SOLR_CONTENT_ID.toUpperCase(), rowkey);
        //docType
        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));
        //Solr唯一主键
        doc.addField(("ID".toUpperCase()), UUID.randomUUID().toString().replace("-", ""));
        //import_time
        doc.addField("import_time".toUpperCase(), new Date());

        return doc;
    }



    /**
     * 对Bcp文件的关键字段进行过滤,
     * 过滤字段为空或者格式不对什么的
     * @param dataRDD JavaRDD<Row>
     * @return JavaRDD<Row>
     */
    private static JavaRDD<Row> filterBcpData(JavaRDD<Row> dataRDD, String task) {
        //Bcp文件对应的字段名
        String[] columns = FieldConstants.BCP_FILE_COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));
        //Bcp文件需要过滤的字段
        Set<String> checkColumns = FieldConstants.FILTER_COLUMN_MAP.get(NamingRuleUtils.getBcpFilterKey(task));
        if ((null == checkColumns) || checkColumns.size() == 0) {
            return dataRDD;
        }

        // 对关键字段进行过滤
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
}


