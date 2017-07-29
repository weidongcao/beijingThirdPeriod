package com.rainsoft.spark;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.JsonUtils;
import net.sf.json.JSONArray;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * Spark处理BCP文件
 * 主要进行两项操作
 * 1.将BCP文件数据导入到Solr
 * 2.将BCP文件数据导入HBase
 * Created by CaoWeiDong on 2017-07-29.
 */
public class FtpSparkOperateBcp {
    private static final Logger logger = LoggerFactory.getLogger(FtpSparkOperateBcp.class);
    private static final String ORACLE_TABLE_NAME = BigDataConstants.ORACLE_TABLE_FTP_NAME;
    private static final String SOLR_URL = ConfigurationManager.getProperty("solr_url");

    //创建Solr客户端
    protected static SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName(FtpSparkOperateBcp.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = ConfigurationManager.getProperty("bcp_file_path");
        path = "file:///" + path;
        JavaRDD<String> originalRDD = sc.textFile(path);
        logger.info("读取数据完毕...");
        JavaRDD<String[]> valueArrrayRDD = originalRDD.flatMap(
                new FlatMapFunction<String, String[]>() {
                    @Override
                    public Iterable<String[]> call(String s) throws Exception {
                        List<String[]> list = new ArrayList<>();
                        String[] cols = s.split(BigDataConstants.BCP_LINE_SEPARATOR);
                        for (String line : cols) {
                            String[] fields = line.split(BigDataConstants.BCP_FIELD_SEPARATOR);
                            long captureTimeLong = DateUtils.TIME_FORMAT.parse(fields[17]).getTime();
                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            String rowkey = captureTimeLong + "_" + uuid;
                            list.add(ArrayUtils.addAll(new String[]{rowkey}, fields));
                        }
                        return list;
                    }
                }
        );

        JSONArray ftpFields = JsonUtils.getJsonValueByFile("oracleTableField.json", ORACLE_TABLE_NAME);
        valueArrrayRDD.checkpoint();
        valueArrrayRDD.foreach(
                new VoidFunction<String[]>() {
                    @Override
                    public void call(String[] strings) throws Exception {
                        System.out.println(StringUtils.join(strings, "\t\t"));
                    }
                }
        );
        /**
         * 数据写入HBase
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = valueArrrayRDD.flatMapToPair(
                new PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String[] strings) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();

                        String rowkey = strings[0];
                        String[] fields = ArrayUtils.subarray(strings, 1, strings.length);
                        for (int i = 0; i < ftpFields.size(); i++) {
                            String value = fields[i];
                            if ((null != value) && (!"".equals(value))) {
                                list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, ftpFields.getString(i)), value));
                            }
                        }
                        return list;
                    }
                }
        ).sortByKey();
        HBaseUtils.writeData2HBase(hfileRDD, BigDataConstants.HBASE_TABLE_PREFIX + ORACLE_TABLE_NAME, BigDataConstants.HBASE_TABLE_FTP_CF, "/tmp/hbase/hfile/http");

        /**
         * 数据写入Solr
         */
        JavaRDD<SolrInputDocument> solrRDD = valueArrrayRDD.mapPartitions(
                new FlatMapFunction<Iterator<String[]>, SolrInputDocument>() {
                    @Override
                    public Iterable<SolrInputDocument> call(Iterator<String[]> iterator) throws Exception {
                        List<SolrInputDocument> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            //FTP数据列数组
                            String[] str = iterator.next();
                            SolrInputDocument doc = new SolrInputDocument();
                            String rowkey = str[0];
                            //SID
                            doc.addField(BigDataConstants.SOLR_CONTENT_ID.toUpperCase(), rowkey);
                            //docType
                            doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, BigDataConstants.SOLR_DOC_TYPE_FTP_VALUE);
                            //capture_time
                            doc.addField("capture_time", rowkey.split("_")[0]);
                            //import_time
                            doc.addField("import_time".toUpperCase(), DateUtils.TIME_FORMAT.format(new Date()));
                            String[] fields = ArrayUtils.subarray(str, 1, str.length);
                            for (int i = 0; i < ftpFields.size(); i++) {
                                doc.addField(ftpFields.getString(i).toUpperCase(), fields[i]);
                            }
                            list.add(doc);
                        }
                        return list;
                    }
                }
        );
        List<SolrInputDocument> solrDocList = solrRDD.collect();
        client.add(solrDocList);

    }
}
