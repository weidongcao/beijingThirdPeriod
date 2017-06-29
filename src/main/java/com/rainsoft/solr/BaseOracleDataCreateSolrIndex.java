package com.rainsoft.solr;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.dao.ImchatDao;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
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
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataCreateSolrIndex.class);

    //一次写入文件的数据量
    protected static final int writeSize = 100000;
    //系统分隔符
    protected static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //数字输出格式
    protected static NumberFormat numberFormat = NumberFormat.getNumberInstance();

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    protected static final String SOLR_URL = "http://192.168.10.11:8080/solr/yisou";

    //创建Solr客户端
    //创建Solr客户端
//    protected static SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();
    protected static CloudSolrClient client = SolrUtil.getSolrClient("yisou");

    //导入记录文件
    protected static File recordFile;

    //导入记录
    protected static Map<String, String> recordMap = new HashMap<>();

    protected static final String FTP = "ftp";
    protected static final String HTTP = "http";
    protected static final String IMCHAT = "imchat";

    protected static final String FTP_TYPE = "文件";
    protected static final String HTTP_TYPE = "网页";
    protected static final String IMCHAT_TYPE = "聊天";

    protected static final String BBS_TYPE = "论坛";
    protected static final String EMAIL_TYPE = "邮件";
    protected static final String SEARCH_TYPE = "搜索";
    protected static final String SHOP_TYPE = "SHOP";
    protected static final String SERVICE_TYPE = "场所";
    protected static final String REAL_TYPE = "真实";
    protected static final String VID_TYPE = "虚拟";
    protected static final String WEIBO_TYPE = "微博";

    protected static final String SUCCESS_STATUS = "success";
    protected static final String FAIL_STATUS = "fail";

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
}
