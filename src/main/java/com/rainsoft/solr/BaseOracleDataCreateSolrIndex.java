package com.rainsoft.solr;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.ReflectUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataCreateSolrIndex.class);

    //一次写入文件的数据量
    protected static final int writeSize = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //数字输出格式
    protected static NumberFormat numberFormat = NumberFormat.getNumberInstance();

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    private static final String SOLR_URL = ConfigurationManager.getProperty("solr_url");

    //创建Solr客户端
    protected static SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();

    //导入记录文件
    static File recordFile;

    //导入记录
    static Map<String, String> recordMap = new HashMap<>();

    static final String SUCCESS_STATUS = "success";
    static final String FAIL_STATUS = "fail";

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

    /**
     * 提交导入Solr索引
     *
     * @param cacheList 要导入Solr的索引
     * @param client    SolrClient
     * @return 提交状态
     */
    public static boolean submitSolr(List<SolrInputDocument> cacheList, SolrClient client) {
        /*
         * 异常捕获
         * 如果失败尝试3次
         */
        int tryCount = 0;
        boolean flat = false;
        while (tryCount < 3) {
            try {
                if (!cacheList.isEmpty()) {
                    client.add(cacheList, 1000);
                }
                flat = true;
                //如果索引成功,跳出循环
                break;
            } catch (Exception e) {
                e.printStackTrace();
                tryCount++;
                flat = false;
            }
        }
        return flat;
    }

    public static boolean delSolrDocTypeByDate(String docType, Date curDate) {
        String templateDelCmd = "docType:${docType} and capture_time:[${startSec} TO ${endSec}]";
        long startSec = curDate.getTime();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(curDate);
        calendar.add(Calendar.DATE, 1);
        long endSec = calendar.getTimeInMillis();
        String delCmd = templateDelCmd.replace("${docType}", docType)
                .replace("${startSec}", startSec+"")
                .replace("${endSec}", endSec+"");

        return SolrUtil.delSolrByCondition(delCmd);
    }

    static void addFieldToSolr(SolrInputDocument doc, Field[] fields, Object obj) {
        //遍历实体属性,将之赋值给Solr导入实体
        for (Field field : fields) {
            String fieldName = field.getName();
            String fieldValue = (String) ReflectUtils.getFieldValueByName(fieldName, obj);
            if (null == fieldValue) {
                fieldValue = "";
            }
            if ("id".equals(fieldName)) {
                doc.addField("SID", fieldValue);
            } else {
                doc.addField(fieldName.toUpperCase(), fieldValue);
            }
        }
        //导入时间
        doc.addField("IMPORT_TIME", com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));
    }

    /**
     * 数据导入结果处理
     * @param type 任务类型
     * @param captureTime 捕获日期
     * @param flat 导入结果
     * @throws IOException 文件写入失败
     */
    static void recordImportResult(String type,String captureTime, boolean flat) throws IOException {
        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_" + type, SUCCESS_STATUS);
        } else {
            recordMap.put(captureTime + "_" + type, FAIL_STATUS);
            logger.error("当天数据导入失败");
        }

        //导入记录Map转List
        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        //对转换后的导入记录List进行排序
        Collections.sort(newRecordList);

        //导入记录List转String
        String newRecords = StringUtils.join(newRecordList, "\r\n");

        //写入导入记录文件
        FileUtils.writeStringToFile(recordFile, newRecords, false);

        logger.info("{} : {} 的数据,索引完成", type, captureTime);
    }
}
