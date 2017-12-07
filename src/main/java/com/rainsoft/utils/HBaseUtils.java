package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.domain.TaskBean;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * HBase工具类
 *
 * @author Cao Wei Dong
 */
public class HBaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    //HBase 配置类
    private static Configuration conf = null;

    //HBase连接
    private static Connection conn = null;

    private static final String zkHost = ConfigurationManager.getProperty("zkHost");

    static {
        if (conn == null) {
            try {
                init();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /**
     * 初始化HBase连接
     */
    private static void init() throws IOException {
        // 获取HBase配置信息
        conf = HBaseConfiguration.create();
        conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 10000);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", zkHost);
        conf.set("zookeeper.session.timeout", 2 * 60 * 60 * 1000 + "");

        //创建HBase连接
        conn = ConnectionFactory.createConnection(conf);
        logger.info("HBase 初始化成功");
    }

    /**
     * 获取HBase表名
     *
     * @param tableName HBase表名
     * @return HBase表
     */
    public static HTable getTable(String tableName) throws Exception {

        Table table = conn.getTable(TableName.valueOf(tableName));
        logger.info("获取HBase表 {} 成功", tableName);
        return (HTable) table;
    }

    /**
     * 获取HBase的配置类
     *
     * @return 返回HBase的配置类
     */
    public static Configuration getConf() {
        return conf;
    }

    /**
     * 获取HBase的连接
     *
     * @return 返回HBase连接
     */
    private static Connection getConn() {
        if (null == conn) {
            try {
                init();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("获取Hbase数据连接失败");
                System.exit(-1);
            }
        }
        return conn;
    }


    /**
     * 向HBase的插入数据的实体Put添加Cell
     * Cell的RowKey、列名、值全部为byte[]
     *
     * @param put   HBase插入一条数据的实体
     * @param cf    HBase的列簇
     * @param col   HBase一个Cell对应的字段名
     * @param value HBase一个Cell的值
     * @return 返回向HBase插入的Put实体
     */
    public static Put addHBasePutColumn(Put put, byte[] cf, byte[] col, byte[] value) {
        put.addColumn(cf, col, value);
        return put;
    }

    /**
     * 向HBase的插入数据的实体Put添加Cell
     *
     * @param put   HBase插入一条数据的实体
     * @param cf    HBase的列簇
     * @param col   HBase一个Cell对应的字段名
     * @param value HBase一个Cell的值
     * @return 返回HBase插入的Put实体
     */

    public static Put addHBasePutColumn(Put put, String cf, String col, String value) {
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        return put;
    }

    /**
     * Spark生成HFile文件并写HBase
     * 分成四步：
     * 第一步：经过二次排序的RDD转为HFile格式的RDD
     * 第二步：删除HDFS文件系统上的输出目录
     * 第三步：生成HFile文件
     * 第四步：HFile文件加载到HBase
     *
     * @param dataRDD      SparkRDD， 其所包含的一条数据为HBase的一条数据的一个Cell
     * @param tablename    HBase表名
     * @param cf           HBase列簇
     * @param tempHDFSPath HFile文件临时保存目录，如果已经存在先删除再创建，导入HBase后再删除
     */
    public static void writeData2HBase(
            JavaPairRDD<RowkeyColumnSecondarySort, String> dataRDD,
            String tablename,
            String cf,
            String tempHDFSPath
    ) {
        logger.info("开始Spark生成HFile文件并写HBase...");
        //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,Value为KeyValue
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRDD = transformSecondarySortToHfileFormat(dataRDD, cf);

        //判断HDFS上是否存在此路径，如果存在删除此路径
        deleteHdfsPath(tempHDFSPath);

        //生成HFile文件并保存到临时目录
        //此处运行完成之后,在临时目录会有我们生成的Hfile文件
        hfileRDD.saveAsNewAPIHadoopFile(
                tempHDFSPath,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                HBaseUtils.getConf()
        );
        logger.info("Spark 生成HBase的{}表的HFile成功,HFile", tablename);

        //加载HFile文件到HBase
        loadHFile(tablename, tempHDFSPath);


    }

    public static void writeData2HBase(JavaPairRDD<RowkeyColumnSecondarySort, String> dataRDD, String task) {

        logger.info("开始Spark生成HFile文件并写HBase...");

        //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,Value为KeyValue
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRDD = transformSecondarySortToHfileFormat(
                dataRDD,
                NamingRuleUtils.getHBaseContentTableCF(task)
        );

        //HFile在HDFS上的临时目录
        String hfileDir = NamingRuleUtils.getHFileTaskDir(task) + UUID.randomUUID().toString().replace("-", "");
        //HBase表名
        String tableName = NamingRuleUtils.getHBaseTableName(task);

        //判断HDFS上是否存在此路径，如果存在删除此路径
        deleteHdfsPath(hfileDir);

        //生成HFile文件并保存到临时目录
        //此处运行完成之后,在临时目录会有我们生成的Hfile文件
        hfileRDD.saveAsNewAPIHadoopFile(
                hfileDir,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                HBaseUtils.getConf()
        );
        logger.info("Spark 生成HBase的{}表的HFile成功,HFile", tableName);

        //加载HFile文件到HBase
        loadHFile(tableName, hfileDir);

    }

    /**
     * 递归删除HDFS上的目录
     *
     * @param dir HDFS路径
     */
    public static void deleteHdfsPath(String dir) {
        Path path = new Path(dir);
        FileSystem fileSystem;
        try {
            fileSystem = path.getFileSystem(HBaseUtils.getConf());
            //判断HDFS上是否存在此路径，如果存在删除此路径
            if (fileSystem.exists(path)) {
                logger.info("删除HDFS上的目录: {}", path);
                //递归删除HDFS上的目录
                //第一个参数:目录路径
                //第二个参数:是否递归删除
                fileSystem.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * 将经过二次排序的RDD转换为HFile文件格式的RDD
     *
     * @param pairRDD 经过干净排序的JavaPairRDD
     * @return hfileRDD
     */
    public static JavaPairRDD<ImmutableBytesWritable, KeyValue> transformSecondarySortToHfileFormat(
            JavaPairRDD<RowkeyColumnSecondarySort, String> pairRDD,
            String cf
    ) {
        //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,Value为KeyValue
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRDD = pairRDD.mapToPair(
                (PairFunction<Tuple2<RowkeyColumnSecondarySort, String>, ImmutableBytesWritable, KeyValue>) tuple2 -> {
                    //rowkey
                    String rowkey = tuple2._1().getRowkey();
                    //字段名
                    String column = tuple2._1().getColumn();
                    //字段值
                    String value = tuple2._2();

                    ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
                    KeyValue kv = new KeyValue(
                            Bytes.toBytes(rowkey),
                            Bytes.toBytes(cf),
                            Bytes.toBytes(column),
                            Bytes.toBytes(value)
                    );
                    return new Tuple2<>(im, kv);
                }
        );

        return hfileRDD;
    }

    /**
     * 加载HFile文件到HBase
     *
     * @param tablename HBase表名
     * @param dir       HFile文件所在路径
     */
    private static void loadHFile(String tablename, String dir) {
        Path path = new Path(dir);
        try {
            //开始导入HBase表
            RegionLocator regionLocator = HBaseUtils.getConn().getRegionLocator(TableName.valueOf(tablename));

            //创建一个hadoop的mapreduce的job
            Job job = Job.getInstance();

            //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);

            //输出文件的内容KeyValue
            job.setMapOutputValueClass(KeyValue.class);

            //根据表名获取表
            HTable table = getTable(tablename);

            //配置HFileOutputFormat2的信息
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

            //创建导入Hbase的对象
            LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(conf);

            //正式开始导入
            logger.info("HFile文件开始导入{}表...", tablename);
            bulkLoader.doBulkLoad(path, table);
            logger.info("HFile文件导入{}表成功", tablename);

            deleteHdfsPath(dir);
            //关闭连接
            IOUtils.closeQuietly(table);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("加载HFile文件失败");
            System.exit(-1);
        }

    }

    /**
     * 根据一条数据及字段名和HBase的RowKey生成HBase的一条数据的多个Cell
     *
     * @param row     一条数据的多个字段
     * @param columns 字段名数组
     * @param rowkey  HBase的RowKey
     * @return 经过二次排序的RDD
     */
    public static List<Tuple2<RowkeyColumnSecondarySort, String>> getHFileCellListByRow(
            Row row,
            String[] columns,
            String rowkey
    ) {
        //返回结果集
        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
        for (int j = 0; j < columns.length; j++) {
            Object value = row.get(j);
            if (null != value) {
                RowkeyColumnSecondarySort sort = new RowkeyColumnSecondarySort(rowkey, columns[j]);
                list.add(new Tuple2<>(sort, value.toString()));
            }
        }
        return list;
    }


    public static void addFields(
            String[] values,
            TaskBean task,
            List<Tuple2<RowkeyColumnSecondarySort, String>> list,
            String rowKey
    ) {
        for (int i = 1; i < values.length; i++) {
            if (task.getColumns().length <= i - 1) {
                break;
            }
            String key = task.getColumns()[i - 1].toUpperCase();
            String value = values[i];
            //如果字段的值为空则不写入HBase
            if ((null != value) && (!"".equals(value))) {
                list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowKey, key), value));
            }
        }
    }

    /**
     * 将数据转为经过二次排序的JavaPairRDD
     *
     * @param javaRDD 源数据
     * @param columns 数据字段名
     * @return 经过二次排序的RDD
     */
    public static JavaPairRDD<RowkeyColumnSecondarySort, String> getHFileRDD(
            JavaRDD<String[]> javaRDD,
            String[] columns
    ) {
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = javaRDD.flatMapToPair(
                (PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>) strings -> {
                    List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                    //获取HBase的RowKey
                    String rowKey = strings[0];
                    //将一条数据转为HBase能够识别的形式
                    for (int i = 1; i < strings.length; i++) {
                        if (i >= columns.length) {
                            break;
                        }
                        String key = columns[i].toUpperCase();
                        String value = strings[i];
                        //如果字段的值为空则不写入HBase
                        if ((null != value) && (!"".equals(value))) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowKey, key), value));
                        }
                    }
                    return list.iterator();
                }
        ).sortByKey();
        return hfileRDD;
    }

    /**
     * 将数据转为经过二次排序的JavaPairRDD
     *
     * @param javaRDD      源数据
     * @param columns      数据字段名
     * @param rowKeyColumn 要被作为rowkey的列
     * @return HFileRDD
     */
    public static JavaPairRDD<RowkeyColumnSecondarySort, String> getHFileRDD(
            JavaRDD<String[]> javaRDD,
            String[] columns,
            String rowKeyColumn
    ) {
        int rowKeyIndex = ArrayUtils.indexOf(columns, rowKeyColumn);
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = javaRDD.flatMapToPair(
                (PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>) values -> {
                    List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                    //获取HBase的RowKey
                    String rowKey = values[rowKeyIndex];
                    //将一条数据转为HBase能够识别的形式
                    for (int i = 0; i < values.length; i++) {
                        //如果数据值的个数超过数据名的个数,超出的舍弃
                        if (i >= columns.length) {
                            break;
                        }
                        //如果是被当作rowkey的列跳过
                        if (i == rowKeyIndex) {
                            continue;
                        }
                        String key = columns[i].toUpperCase();
                        String value = values[i];
                        //如果字段的值为空则不写入HBase
                        if (StringUtils.isNotBlank(value)) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowKey, key), value));
                        }
                    }
                    return list.iterator();
                }
        ).sortByKey();
        return hfileRDD;
    }

    /**
     * 生成HBase的RowKey的前缀
     * RowKey由两部分组成，Rowkey的前缀和唯一标识符
     * 前缀由两部分组成
     * 第一部分: 一个两位的随机数,对数据进行散列
     * 第二部分:
     * 1. 对当前时间的毫秒数进行位运算(相当于除以1024)以减小时间戳的长度（命名为second）
     * 2. 获取比以上值高一个数量级的最小值,比如说second是14亿,它的数量级就是十亿级的，
     * 比它高一个数量级就是百亿级的，百亿级的最小值就是100亿（命名为min）
     * 2. 用min 减去second，从而使最新插入的数据排的前面
     * 3. 将以上结果转为16进制
     * 这样做有以下考虑：
     * 1. 现在的时间戳转秒数按位与运算省去了大量的计算，但是结果是差不多的
     * 2. 将时间戳转为秒数，再转为16进制大大减小了rowkey的长度
     * 3. 当时时间的时间戳是10亿级的，就是再过100年，也不会达到百亿级，这样字符的长度都是固定的
     * 这样做有以下好处：
     * 1. 对HBase进行负载均衡，对数据进行散列，当前插入的数就会随机地分散到每一个Region
     * 2. 对导入时间转时间戳再转秒数然后再转16进制
     * 使rowkey不会过长，但是同时不同Region中同一时间段内的数据都集中到了一起，提高HBase查询的速度
     *
     * @param date 导入时间
     * @return rowkeyPrefix rowkey前缀
     */
    public static String createRowKeyPrefix(Date date) {
        //生成散列
        String hashPrefix = RandomStringUtils.randomAlphabetic(2);
        //根据毫秒数按位与运算，向右移10位，相当于除了1024
        Long second = date.getTime() >> 10;
        //获取second的位数
        String zeros = (second + "").replaceAll("\\d", "0");
        //生成秒数的极大值，比如说当前是1秒，极大值就是10秒，当前值是100秒，极大值就是1000秒，以此类推，也就是说极大值比当前秒数高一个数量级
        Long bigSecond = Long.valueOf("1" + zeros);
        //最大秒秒数送去当前日期的秒数，然后转16进制
        String hex = Long.toHexString(bigSecond - second);

        //生成rowkey前缀
        String rowkeyPrefix = hashPrefix + hex;
        return rowkeyPrefix;
    }

    /**
     * rowkey由两部分组成
     * rowkwy的前缀和唯一性标识符
     * 此唯一性标识符随机生成4位大小写字母及数字组成
     * 这样算起来4位，有1477万多种组合
     * <p>
     * 通过上面的Rowkey前缀和唯一性标识符组合的设计有以下好处：
     * 1. 每一个Rowkey都是唯一的
     * 2. 每一个Rowkey的前两位都是散列字段，后4位都是唯一性标识符，中间9位都是导入时间的转化
     * 3. 避免了热点数据聚焦到同一个Region里
     * 4. Rowkey的长度减小到了15个字节
     *
     * @param list 标识符集合
     * @return 生成list集合中不包含的指定长度的标识符
     */
    public static String createRowkeyIdentifier(List<String> list) {
        String identifier = RandomStringUtils.randomAlphanumeric(4);
        if (null != list) {
            while (list.contains(identifier)) {
                identifier = RandomStringUtils.randomAlphanumeric(4);
                if (list.contains(identifier) != true) {
                    list.add(identifier);
                    break;
                }
            }
        }
        return identifier;

    }

    public static void main(String[] args) throws Exception {

        System.out.println(createRowKeyPrefix(new Date()) + createRowkeyIdentifier(new ArrayList<>()));
    }
}
