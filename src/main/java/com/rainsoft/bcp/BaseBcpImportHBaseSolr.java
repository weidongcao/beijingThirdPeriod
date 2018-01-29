package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.*;
import com.rainsoft.utils.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
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
    public static final String bcpPoolDir = ConfigurationManager.getProperty("bcp.receive.dir");
    //Bcp文件移动的个数
    public static final String operatorBcpNumber = ConfigurationManager.getProperty("operator.bcp.number");
    //要移动到的目录
    public static final String bcpFilePath = ConfigurationManager.getProperty("bcp.file.path");
    //是否导入到HBase
    public static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");


    /**
     * 将Bcp文件的内容导入到HBase、Solr
     */
    public static List<String> getFileContent(File file) {
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
     * Bcp数据导入到Solr
     *
     * @param javaRDD JavaRDD
     * @param task    任务类型
     */
    public static void bcpWriteIntoSolr(JavaRDD<Row> javaRDD, String task) {
        logger.info("开始将 {} 的BCP数据索引到Solr", task);
        //Bcp文件对应的字段名
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task));

        /*
         * 数据写入Solr
         */
        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> list = new ArrayList<>();
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        String rowkey = row.getString(0);
                        //SID
                        doc.addField(BigDataConstants.SOLR_CONTENT_ID.toUpperCase(), rowkey);

                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));

                        //Solr唯一主键
                        doc.addField(("ID".toUpperCase()), UUID.randomUUID().toString().replace("-", ""));

                        //import_time
                        Date curDate = new Date();
                        doc.addField("import_time".toUpperCase(), DateFormatUtils.SOLR_FORMAT.format(curDate));

                        SolrUtil.addBcpIntoSolrInputDocument(columns, row, doc);
                        list.add(doc);

                    }
                    if (list.size() > 0) {
                        //写入Solr
                        SolrUtil.setCloudSolrClientDefaultCollection(client, new Date());
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
    public static void bcpWriteIntoHBase(JavaRDD<Row> javaRDD, String task) {
        logger.info("{}的BCP数据开始写入HBase...", task);

        //将数据转为可以进行二次排序的形式
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(
                javaRDD,
                FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey(task))
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
    public static void moveBcpfileToWorkDir(String task, String shellMvTemplate) {

        String shellMv = shellMvTemplate.replace("${bcp_pool_dir}", bcpPoolDir)
                .replace("${task}", task)
                .replace("${operator_bcp_number}", operatorBcpNumber)
                .replace("${bcp_file_path}", bcpFilePath);

        LinuxUtils.execShell(task, shellMv);
    }
}


