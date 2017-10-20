package com.rainsoft.hive;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Spark将通过Hive将HBase的数据索引到Solr
 * Created by CaoWeiDong on 2017-07-25.
 */
public class SparkExportSolr {
    private static final Logger logger = LoggerFactory.getLogger(SparkExportSolr.class);

    private static final String SOLR_URL = ConfigurationManager.getProperty("solr.url");

    //创建Solr客户端
    protected static SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();
//    protected static CloudSolrClient client = SolrUtil.getClusterClient("yisou");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(SparkExportSolr.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext();
        HiveContext sqlContext = new HiveContext(sc.sc());
        DataFrame jdbcDF = sqlContext.sql("select * from emp");

        JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD();
        String[] columns = jdbcDF.columns();
        System.out.println(StringUtils.join(columns, "\t"));
        jdbcRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row.mkString());
                    }
                }
        );

        JavaRDD<SolrInputDocument> solrRDD = jdbcRDD.map(
                new Function<Row, SolrInputDocument>() {
                    @Override
                    public SolrInputDocument call(Row row) throws Exception {
                        SolrInputDocument doc = new SolrInputDocument();
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        doc.addField("id", uuid);
                        for (int i = 0; i < columns.length; i++) {
                            String key = columns[i];
                            Object value = row.get(i);
                            if ("id".equals(key)) {
                                key = "sid";
                            }
                            if (value != null) {
                                doc.addField(key, row.get(i).toString());
                            }
                        }
                        return doc;
                    }
                }
        );
//        BaseOracleDataExport.submitSolr(solrRDD.collect(), client);
    }
}
