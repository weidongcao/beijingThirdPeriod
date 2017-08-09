package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
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
//    public static CloudSolrClient client = SolrUtil.getClusterSolrClient();
    //    public static HttpSolrClient client = new HttpSolrClient.Builder(ConfigurationManager.getProperty("solr_url")).build();
    //BCP文件路径
    private String bcpPath;
    //HBase表名
    private String hbaseTableName;
    //HBase列簇
    private String hbaseCF;
    //HFile在HDFS上的临时存储目录
    private String hfileTmpStorePath;
    //数据类型
    private String contentType;
    //获取Oracle表字段名
    private String[] fields;
    //Sorl的docType
    private String docType;
    public static final long curTimeLong = new Date().getTime();
    //捕获时间在在bcp文件里一行的位置（第一个从0开始）
    private int CaptureTimeIndexBcpFileLine;

    public void run(JavaSparkContext sc) {
        logger.info("开始处理 {} 的BCP数据", getContentType());
        JavaRDD<String> originalRDD = sc.textFile(getBcpPath());

        //对BCP文件数据进行基本的处理，并生成ID(HBase的RowKey，Solr的Sid)
        JavaRDD<String[]> valueArrrayRDD = originalRDD.flatMap(
                new FlatMapFunction<String, String[]>() {
                    @Override
                    public Iterable<String[]> call(String s) throws Exception {
                        List<String[]> list = new ArrayList<>();
                        //对一行的数据按字段分隔符进行切分
                        String[] cols = s.split(BigDataConstants.BCP_LINE_SEPARATOR);
                        for (String line : cols) {
                            //字段值列
                            String[] values = line.split(BigDataConstants.BCP_FIELD_SEPARATOR);
                            //生成唯一ID(HBase的RowKey，Solr的Sid)
                            long captureTimeLong;
                            try {
                                captureTimeLong = DateUtils.TIME_FORMAT.parse(values[getCaptureTimeIndexBcpFileLine()]).getTime();
                            } catch (Exception e) {
                                continue;
                            }
                            if (captureTimeLong > curTimeLong) {
                                continue;
                            }
                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            String rowkey = captureTimeLong + "_" + uuid;
                            //生成新数据写加入到list
                            list.add(ArrayUtils.addAll(new String[]{rowkey}, values));
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
                    if (getFields().length + 1 == strings.length) {
                        //BCP文件 没有新加字段，
                        return true;
                    } else if ((getFields().length + 1) == (strings.length + 3)) {
                        //BCP文件添加了新的字段，且只添加了三个字段
                        return true;
                    } else if (BigDataConstants.CONTENT_TYPE_HTTP.equalsIgnoreCase(getContentType()) &&
                            ((getFields().length + 1) == (strings.length + 3 + 4))) {
                        //HTTP的BCP文件添加了新的字段，且添加了7个字段
                        return true;
                    }
                    return false;
                }
        ).repartition(4);
        //缓存数据
        filterValuesRDD.cache();

        //BCP文件数据写入HBase
        bcpWriteIntoHBase(filterValuesRDD);

        //BCP文件数据写入Solr
        bcpWriteIntoSolr(filterValuesRDD);

        logger.info("<---------------------------------------------- {}的BCP数据处理完毕 ---------------------------------------------->", getContentType());
    }

    public void bcpWriteIntoSolr(JavaRDD<String[]> javaRDD) {
        logger.info("开始将 {} 的BCP数据索引到Solr", getContentType());

        String docType = getDocType();
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
                                String key = getFields()[i].toUpperCase();
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
                            logger.info("{} 此Spark Partition 数据为空", getContentType());
                        }
                    }
                }
        );
        logger.info("####### {}的BCP数据索引Solr完成 #######", getContentType());
    }

    public void bcpWriteIntoHBase(JavaRDD<String[]> javaRDD) {
        logger.info("{}的BCP数据开始写入HBase...", getContentType());

        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = javaRDD.flatMapToPair(
                new PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String[] strings) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();

                        //获取HBase的RowKey
                        String rowkey = strings[0];
                        String[] values = ArrayUtils.subarray(strings, 1, strings.length);
                        for (int i = 0; i < values.length; i++) {
                            String key = getFields()[i].toUpperCase();
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
        HBaseUtils.writeData2HBase(hfileRDD, getHbaseTableName(), getHbaseCF(), getHfileTmpStorePath());
        logger.info("####### {}的BCP数据写入HBase完成 #######", getContentType());
    }

    /**
     * 替换BCP数据文件中非正常的回车换行
     *
     * @param path
     */
    public void replaceFileRN(String path) {
        File dir = FileUtils.getFile(path);

        File[] files = dir.listFiles();
        for (File file : files) {
            try {
                String content = FileUtils.readFileToString(file, "utf-8");
                content = content.replace("\r\n", "")   //替换Win下的换行
                        .replace("\n", "")              //替换Linux下的换行
                        .replace("\r", "");              //替换Mac下的换行
                content = content.replaceAll(BigDataConstants.BCP_LINE_SEPARATOR, BigDataConstants.BCP_LINE_SEPARATOR + "\r\n");
                FileUtils.writeStringToFile(file, content, "utf-8", false);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        logger.info("回车换行符替换完成,替换路径： {}", path);
    }

    public static void main(String[] args) {
        new SparkOperateBcp().replaceFileRN("E:\\work\\RainSoft\\data\\im_chat");
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }

    public void setHbaseTableName(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    public String getHbaseCF() {
        return hbaseCF;
    }

    public void setHbaseCF(String hbaseCF) {
        this.hbaseCF = hbaseCF;
    }

    public String getHfileTmpStorePath() {
        return hfileTmpStorePath;
    }

    public void setHfileTmpStorePath(String hfileTmpStorePath) {
        this.hfileTmpStorePath = hfileTmpStorePath;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public int getCaptureTimeIndexBcpFileLine() {
        return CaptureTimeIndexBcpFileLine;
    }

    public void setCaptureTimeIndexBcpFileLine(int captureTimeIndexBcpFileLine) {
        CaptureTimeIndexBcpFileLine = captureTimeIndexBcpFileLine;
    }

    public String getBcpPath() {
        return bcpPath;
    }

    public void setBcpPath(String bcpPath) {
        this.bcpPath = bcpPath;
    }

    @Override
    public String toString() {
        return "SparkOperateBcp{" +
                "bcpPath='" + bcpPath + '\'' +
                ", hbaseTableName='" + hbaseTableName + '\'' +
                ", hbaseCF='" + hbaseCF + '\'' +
                ", hfileTmpStorePath='" + hfileTmpStorePath + '\'' +
                ", contentType='" + contentType + '\'' +
                ", fields=" + fields +
                ", docType='" + docType + '\'' +
                ", CaptureTimeIndexBcpFileLine=" + CaptureTimeIndexBcpFileLine +
                '}';
    }
}

