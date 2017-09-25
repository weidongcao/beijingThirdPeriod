package com.rainsoft.hbase.solr;

import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by CaoWeiDong on 2017-09-24.
 */
public class BbsImportSolr {
    private static SolrClient client = SolrUtil.getClusterSolrClient();
    private static int batchCount = ConfigurationManager.getInteger("commit.solr.count");
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_content_bbs");
    private static String TABLE_NAME = "H_REG_CONTENT_BBS_TMP";
    private static final String CF = "CONTENT_BBS";
    private static final DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Configuration hbaseConf = HBaseUtils.getConf();
        //设置查询的表名
        hbaseConf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

        SparkConf conf = new SparkConf()
                .setAppName(BbsImportSolr.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                hbaseConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        hbaseRDD.cache();
        System.out.println("开始时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date() + "总条目：<<<<<<<<<<<<<<<<<<<<<<<<" + hbaseRDD.count());

        List<SolrInputDocument> docList = new ArrayList<>();
        hbaseRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                    @Override
                    public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iter) throws Exception {

                        while (iter.hasNext()) {
                            SolrInputDocument doc = new SolrInputDocument();
                            Tuple2<ImmutableBytesWritable, Result> tuple = iter.next();
                            Result result = tuple._2;

                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            doc.addField("ID", uuid);
                            doc.addField("docType", "论坛");
                            doc.addField("SID", Bytes.toString(result.getRow()));
                            for (int i = 1; i < columns.length; i++) {
                                String value = Bytes.toString(result.getValue(CF.getBytes(), columns[i].toUpperCase().getBytes()));
                                if (StringUtils.isNotBlank(value)) {
                                    doc.addField(columns[i].toUpperCase(), value);
                                }
                                if (columns[i].equalsIgnoreCase("capture_time")) {
                                    doc.addField("capture_time", timeFormat.parse(value).getTime());
                                }
                            }
                            docList.add(doc);
                            if (!docList.isEmpty() && (docList.size() >= batchCount)) {
                                client.add(docList, 10000);
                                docList.clear();
                            }
                        }
                        if (!docList.isEmpty()) {
                            client.add(docList, 10000);
                        }
                    }
                }
        );

        System.out.println("结束时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date());

    }
}
