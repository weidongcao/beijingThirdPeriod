package com.rainsoft.solr.base;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.inter.InfoDaoInter;
import com.rainsoft.utils.NamingUtils;
import com.rainsoft.utils.SolrUtil;
import com.rainsoft.utils.ThreadUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by CaoWeiDong on 2018-04-18.
 */
public class OracleDistinctDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(OracleDistinctDataExport.class);

    /**
     * 从数据库抽取数据到Solr、HBase
     * 主要做三件事件：
     * 1. 抽取并导入数据
     * 2. 监控抽取
     * 3. 记录抽取情况
     * <p>
     * 根据开始ID，及步长从数据库里取指定数量的数据
     *
     * @param task 任务类型
     * @param dao  数据库连接dao
     */
    public static void extract(String task, InfoDaoInter dao) {
        //监控执行情况
        watch.start();

        Optional<Long> id = getTaskStartId(recordMap, task);
        //首次抽取获取开始的ID
        if (!id.isPresent())
            id = getStartID(dao);

        //根据起始ID及步长从数据库里查询指定数据的数据
        List<String[]> list = dao.getDatasByStartIDWithStep(id);

        //记录从Oracle取到了多少数据
        extractTastCount.put(task, list.size());
        logger.info("从 {} 取到到 {} 条数据", NamingUtils.getTableName(task), list.size());

        //根据不同的情况进行抽取
        extractDataOnCondition(list, task);

        //一次任务抽取完之后需要做的事情
        extractTaskOver(task, id.get() + list.size());
    }

    /**
     * 从数据库中获取从指定时间开始最小的ID
     * 如果是内容的,比如说Bbs，Email，Ftp，Http，Imchat等从三个月前开始最小的ID
     * 如果是信息表的，比如说真实、虚拟、场所等没有时间限制，从最小的开始导入
     *
     * @param dao 数据库连接DAO层
     * @return 抽取起始ID
     */
    private static Optional<Long> getStartID(InfoDaoInter dao) {
        Optional<Long> id = Optional.empty();
        if (dao instanceof InfoDaoInter) {
            //最小的ID，没有时间限制
            id = dao.getMinId();
        }
        return id;
    }

    /**
     * Oracle内容表数据导入到Solr
     *
     * @param javaRDD JavaRDD<String[]>
     * @param task    任务名
     */
    private static void export2Solr(JavaRDD<Row> javaRDD, String task) {
        //字段名数组
        String[] columns = FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(NamingUtils.getTableName(task));
        List<Integer> indexs = getKeyfieldIndexs(columns, FieldConstants.TASK_KEY_MAP.get(task));

        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> docList = new ArrayList<>();
                    //导入时间，集群版Solr需要根据导入时间确定具体导入到哪个Collection
                    while (iterator.hasNext()) {
                        //数据列数组
                        Row row = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        //通过MD5加密生成Sokr唯一ID
                        String id = getMd5ByKeyFields(row, indexs);

                        //ID
                        doc.addField("ID", id);
                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));

                        for (int i = 0; i < row.length(); i++) {
                            //如果字段下标越界,跳出循环
                            if (i >= columns.length)
                                break;
                            SolrUtil.addSolrFieldValue(doc, columns[i].toUpperCase(), row.getString(i));
                        }
                        docList.add(doc);
                        //docList的size达到指定大小时写入到Solr
                        //如果是集群版Solr的话根据捕获时间动态写入到相应的Collection
                        if (writeSize <= docList.size())
                            SolrUtil.submitToSolr(client, docList, writeSize, Optional.of("yisou"));
                    }
                    SolrUtil.submitToSolr(client, docList, 1, Optional.of("yisou"));
                }
        );
        logger.info("####### {}的数据索引Solr完成 #######", NamingUtils.getTableName(task));
    }

    /**
     * 根据数据及任务类型处理不同的抽取情况
     * 如果任务有数据则进行抽取，如果没有数据的话判断是否需要休眠（所有的任务都没有数据）
     *
     * @param list 要抽取的数据
     * @param task 任务类型
     */
    public static void extractDataOnCondition(List<String[]> list, String task) {
        if (list.size() > 0) {
            logger.info("{}表数据抽取任务开始", NamingUtils.getTableName(task));
            //根据数据量决定启动多少个进程
            int threadNum = list.size() / 200000 + 1;
            JavaRDD<Row> javaRDD = getSparkContext().parallelize(list, threadNum)
                    .map((Function<String[], Row>) RowFactory::create);

            //数据持久化
            javaRDD.persist(StorageLevel.MEMORY_ONLY());

            //抽取到Solr
            export2Solr(javaRDD, task);

            //抽取到HBase
            if (isExport2HBase)
                export2HBase(javaRDD, task);

            //取消持久化
            javaRDD.unpersist();

            logger.info("{}表一次数据抽取任务完成...", NamingUtils.getTableName(task));
        } else {
            //如果所有任务的数据都为0, 则程序休眠10分钟
            int cnt = extractTastCount.values().stream().mapToInt(i -> i).sum();
            if ((extractTastCount.size() >= 7) && (cnt == 0)) {
                //Windows下主要用于测试，休眠的时间短些，Linux下主要用于生产，休眠的时间长些
                int seconds;
                String os = System.getProperty("os.name");
                if (os.toLowerCase().startsWith("win")) {
                    seconds = 5;
                } else {
                    seconds = threadSleepSeconds;
                }
                logger.info("Oracle数据所有表都没有抽取到数据，开始休眠，休眠时间: {}", seconds);
                ThreadUtils.programSleep(seconds);
            }
        }
    }

    /**
     * 从字段名称数组中找出关键字段的下标
     * 返回关键字段下标的列表
     * 主要用于根据关键字段生成唯一ID
     *
     * @param fields    该类型所有字段名称
     * @param keyfields 该类型关键字段名称
     * @return 字段在
     */
    public static List<Integer> getKeyfieldIndexs(String[] fields, String[] keyfields) {
        List<Integer> indexs = new ArrayList<>();
        int index;
        for (String field : keyfields) {
            index = ArrayUtils.indexOf(fields, field);
            indexs.add(index);
        }
        return indexs;
    }
}
