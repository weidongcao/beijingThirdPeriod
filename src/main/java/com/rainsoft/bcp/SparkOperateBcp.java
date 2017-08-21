package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.domain.TaskBean;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Spark处理BCP文件
 * 主要进行两项操作
 * 1.将BCP文件数据导入到Solr
 * 2.将BCP文件数据导入HBase
 * Created by CaoWeiDong on 2017-07-29.
 */
public class SparkOperateBcp implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkOperateBcp.class);

    public static void run(TaskBean task) {
        logger.info("开始处理 {} 的BCP数据", task.getContentType());
        SparkConf conf = new SparkConf()
                .setAppName(task.getContentType());
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile(task.getBcpPath());

        //对BCP文件数据进行基本的处理，并生成ID(HBase的RowKey，Solr的Sid)
        JavaRDD<String[]> valueArrrayRDD = originalRDD.mapPartitions(
                new FlatMapFunction<Iterator<String>, String[]>() {
                    @Override
                    public Iterable<String[]> call(Iterator<String> iter) throws Exception {
                        List<String[]> list = new ArrayList<>();
                        while (iter.hasNext()) {
                            String str = iter.next();
                            String[] fields = str.split("\t");
                            list.add(fields);
                        }
                        return list;
                    }
                }
        );
        /*
         * 对数据进行过滤
         * 字段名数组里没有id字段(HBase的RowKey，Solr的Side)
         * BCP文件可能升级，添加了新的字段
         * FTP、IM_CHAT表新加了三个字段："service_code_out", "terminal_longitude", "terminal_latitude"
         * HTTP表新了了7个字段其中三个字段与上面相同："service_code_out", "terminal_longitude", "terminal_latitude"
         *      另外4个字段是："manufacturer_code", "zipname", "bcpname", "rownumber", "
         * 故过滤的时候要把以上情况考虑进去
         */
        JavaRDD<String[]> filterValuesRDD = valueArrrayRDD.filter(
                (Function<String[], Boolean>) strings -> {
                    if (task.getFields().length + 1 == strings.length) {
                        //BCP文件 没有新加字段，
                        return true;
                    } else if ((task.getFields().length + 1) == (strings.length + 3)) {
                        //BCP文件添加了新的字段，且只添加了三个字段
                        return true;
                    } else if (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(task.getContentType()) &&
                            ((task.getFields().length + 1) == (strings.length + 3 + 4))) {
                        //HTTP的BCP文件添加了新的字段，且添加了7个字段
                        return true;
                    }
                    return false;
                }
        ).repartition(4);
        //BCP文件数据写入HBase
        bcpWriteIntoHBase(filterValuesRDD, task);

        sc.close();
    }

    public static void bcpWriteIntoSolr(JavaRDD<String[]> javaRDD, TaskBean task) {
        logger.info("开始将 {} 的BCP数据索引到Solr", task.getContentType());

        String docType = task.getDocType();
        /*
         * 数据写入Solr
         */
        javaRDD.foreachPartition(
                new VoidFunction<Iterator<String[]>>() {
                    @Override
                    public void call(Iterator<String[]> iterator) throws Exception {
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
                            doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, docType);
                            //capture_time
                            doc.addField("capture_time", rowkey.split("_")[0]);
                            //import_time
                            doc.addField("import_time".toUpperCase(), DateUtils.TIME_FORMAT.format(new Date()));
                            String[] values = ArrayUtils.subarray(str, 1, str.length);
                            for (int i = 0; i < values.length; i++) {
                                String value = values[i];
                                String key = task.getFields()[i].toUpperCase();
                                //如果字段的值为空则不写入Solr
                                if ((null != value) && (!"".equals(value))) {
                                    if (!"FILE_URL".equalsIgnoreCase(key) && !"FILE_SIZE".equalsIgnoreCase(key)) {
                                        doc.addField(key, value);
                                    }
                                }
                            }
                            list.add(doc);

                        }
                        if (list.size() > 0) {
                            //写入Solr
                            CloudSolrClient client = SolrUtil.getClusterSolrClient();
                            SolrUtil.submit(list, client);
                            list.clear();
                            SolrUtil.closeSolrClient(client);
                            logger.info("写入Solr成功...");
                        } else {
                            logger.info("{} 此Spark Partition 数据为空", task.getContentType());
                        }
                    }
                }
        );
        logger.info("####### {}的BCP数据索引Solr完成 #######", task.getContentType());
    }

    public static void bcpWriteIntoHBase(JavaRDD<String[]> javaRDD, TaskBean task) {
        logger.info("{}的BCP数据开始写入HBase...", task.getContentType());

        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = javaRDD.flatMapToPair(
                new PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String[] strings) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();

                        //获取HBase的RowKey
                        String rowkey = strings[0];
                        String[] values = ArrayUtils.subarray(strings, 1, strings.length);
                        for (int i = 0; i < values.length; i++) {
                            String key = task.getFields()[i].toUpperCase();
                            String value = values[i];
                            //如果字段的值为空则不写入HBase
                            if ((null != value) && (!"".equals(value))) {
                                list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, key), value));
                            }
                        }
                        list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, "import_time".toUpperCase()), DateUtils.TIME_FORMAT.format(new Date())));
                        return list;
                    }
                }
        ).sortByKey();
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task.getHbaseTableName(), task.getHbaseCF(), task.getHfileTmpStorePath());
        logger.info("####### {}的BCP数据写入HBase完成 #######", task.getContentType());
    }



    public static void main(String[] args) {
//        new SparkOperateBcp().replaceFileRN("E:\\work\\RainSoft\\data\\im_chat");
    }

}

