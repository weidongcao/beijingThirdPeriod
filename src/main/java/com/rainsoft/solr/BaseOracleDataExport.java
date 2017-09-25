package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataExport.class);

    //一次写入文件的数据量
    protected static final int writeSize = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //数字输出格式
    protected static NumberFormat numberFormat = NumberFormat.getNumberInstance();

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    //创建Solr客户端
    protected static SolrClient client = SolrUtil.getClusterSolrClient();

    public static DateFormat hourDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    public static final DateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //一次处理多少小时的数据
    public static final int hourOffset = ConfigurationManager.getInteger("oracle.capture.time.batch");

    //导入记录文件
    static File recordFile;

    //导入记录
    public static Map<String, String> recordMap = new HashMap<>();

    public static final String SUCCESS_STATUS = "success";
    public static final String FAIL_STATUS = "fail";

    static {
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();

        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        if (!recordFile.exists()) {
            try {
                recordFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //导入记录
        List<String> recordsList = null;
        try {
            recordsList = FileUtils.readLines(recordFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //导入记录转为Map
        assert recordsList != null;
        for (String record : recordsList) {
            String[] kv = record.split("\t");
            recordMap.put(kv[0], kv[1]);
        }

        logger.info("程序初始化完成...");
    }
    public static boolean oracleContentTableDataExportSolr(List<String[]> list, String contentType) {
        logger.info("当前要索引的数据量 = {}", numberFormat.format(list.size()));

        //字段名数组
        String[] columns = FieldConstants.COLUMN_MAP.get(contentType);
        //获取时间的位置
        int captureTimeIndex = ArrayUtils.indexOf(columns, BigDataConstants.CAPTURE_TIME);

        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        //数据索引结果状态
        boolean flat = true;

        //向Solr提交数据的次数
        int submitCount = 0;
        //进行Solr索引
        while (list.size() > 0) {
            /*
             * 从Oracle查询出来的数据量很大,每次从查询出来的List中取一定量的数据进行索引
             */
            List<String[]> sublist;
            int startIndex = submitCount * writeSize;
            int endIndex = (submitCount + 1) * writeSize;
            if (endIndex > list.size()) {
                endIndex = list.size();
            }
            sublist = list.subList(startIndex, endIndex);

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (String[] line : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                //生成Solr的唯一ID
                String uuid = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", uuid);

                //添加FTP数据类型为文件
                doc.addField("docType", FieldConstants.DOC_TYPE_MAP.get(contentType));

                for (int i = 0; i < line.length; i++) {
                    String colValue = line[i];
                    //如果字段值为空跳过
                    if (StringUtils.isBlank(colValue)) {
                        continue;
                    }
                    //如果字段下标越界,跳出循环
                    if (i >= columns.length) {
                        break;
                    }

                    if ("id".equalsIgnoreCase(columns[i])) {
                        doc.addField("SID", colValue);
                    } else {
                        doc.addField(columns[i].toUpperCase(), colValue);
                    }

                }

                //捕获时间转为毫秒赋值给Solr导入实体
                try {
                    doc.addField(
                            BigDataConstants.CAPTURE_TIME.toLowerCase(),
                            TIME_FORMAT.parse(line[captureTimeIndex].split("\\.")[0]).getTime());
                } catch (Exception e) {
                    logger.info("{}采集时间转换失败,采集时间为： {}", contentType, line[captureTimeIndex]);
                    e.printStackTrace();
                }

                //索引实体添加缓冲区
                cacheList.add(doc);
            }

            //提交到Solr
            try {
                client.add(cacheList, 1000);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Solr 索引失败");
            }

            //清空历史索引数据
            cacheList.clear();

            //提交次数增加
            submitCount++;

            int tempSubListSize = sublist.size();
            logger.info(
                    "第 {} 次索引 {} 条数据成功;剩余未索引的数据: {}条",
                    submitCount,
                    numberFormat.format(tempSubListSize),
                    numberFormat.format(list.size() - startIndex));
        }

        return flat;
    }
    /**
     * 数据导入结果处理
     * 将导入结果记录到文件
     * @param type 任务类型
     * @param captureTime 捕获日期
     * @param flat 导入结果
     * @throws IOException 文件写入失败
     */
    static void recordImportResult(String type,String captureTime, boolean flat) {
        //数据索引结果成功或者失败写入记录文件,
        String newRecords;
        if (flat) {
            recordMap.put(captureTime + "_" + type, SUCCESS_STATUS);
            newRecords = captureTime + "_" + type + "\t" + SUCCESS_STATUS;
        } else {
            recordMap.put(captureTime + "_" + type, FAIL_STATUS);
            newRecords = captureTime + "_" + type + "\t" + FAIL_STATUS;
            logger.error("当天数据导入失败");
        }

        //写入导入记录文件
        try {
            FileUtils.writeStringToFile(recordFile, newRecords, "utf-8", true);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入导入失败:{}", newRecords);
            System.exit(-1);
        }

        logger.info("{} : {} 的数据,索引完成", type, captureTime);
    }
}
